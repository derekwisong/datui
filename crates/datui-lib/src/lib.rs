use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use polars::datatypes::AnyValue;
use polars::datatypes::DataType;
#[cfg(feature = "cloud")]
use polars::io::cloud::{AmazonS3ConfigKey, CloudOptions};
use polars::prelude::{col, len, DataFrame, LazyFrame, Schema};
#[cfg(feature = "cloud")]
use polars::prelude::{PlPathRef, ScanArgsParquet};
use std::path::{Path, PathBuf};
use std::sync::{mpsc::Sender, Arc};
use widgets::info::{
    read_parquet_metadata, DataTableInfo, InfoContext, InfoFocus, InfoModal, InfoTab,
    ParquetMetadataCache,
};

use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};

use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, Gauge, List, ListItem, Paragraph, Row, StatefulWidget,
    Table, Tabs,
};

pub mod analysis_modal;
pub mod cache;
pub mod chart_data;
pub mod chart_export;
pub mod chart_export_modal;
pub mod chart_modal;
pub mod cli;
pub mod config;
pub mod error_display;
pub mod export_modal;
pub mod filter_modal;
mod help_strings;
pub mod pivot_melt_modal;
mod query;
pub mod sort_filter_modal;
pub mod sort_modal;
mod source;
pub mod statistics;
pub mod template;
pub mod widgets;

pub use cache::CacheManager;
pub use cli::Args;
pub use config::{
    rgb_to_256_color, rgb_to_basic_ansi, AppConfig, ColorParser, ConfigManager, Theme,
};

use analysis_modal::{AnalysisModal, AnalysisProgress};
use chart_export::{
    write_box_plot_eps, write_box_plot_png, write_chart_eps, write_chart_png, write_heatmap_eps,
    write_heatmap_png, BoxPlotExportBounds, ChartExportBounds, ChartExportFormat,
    ChartExportSeries,
};
use chart_export_modal::{ChartExportFocus, ChartExportModal};
use chart_modal::{ChartFocus, ChartKind, ChartModal, ChartType};
pub use error_display::{error_for_python, ErrorKindForPython};
use export_modal::{ExportFocus, ExportFormat, ExportModal};
use filter_modal::{FilterFocus, FilterOperator, FilterStatement, LogicalOperator};
use pivot_melt_modal::{MeltSpec, PivotMeltFocus, PivotMeltModal, PivotMeltTab, PivotSpec};
use sort_filter_modal::{SortFilterFocus, SortFilterModal, SortFilterTab};
use sort_modal::{SortColumn, SortFocus};
pub use template::{Template, TemplateManager};
use widgets::controls::Controls;
use widgets::datatable::{DataTable, DataTableState};
use widgets::debug::DebugState;
use widgets::export;
use widgets::pivot_melt;
use widgets::template_modal::{CreateFocus, TemplateFocus, TemplateModal, TemplateModalMode};
use widgets::text_input::{TextInput, TextInputEvent};

/// Application name used for cache directory and other app-specific paths
pub const APP_NAME: &str = "datui";

/// Re-export compression format from CLI module
pub use cli::CompressionFormat;

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
            // When the lib is in crates/datui-lib, repo root is CARGO_MANIFEST_DIR/../..
            let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
            let sample_data_dir = repo_root.join("tests/sample-data");

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
                // Get the path to the Python script (at repo root)
                let script_path = repo_root.join("scripts/generate_sample_data.py");
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
            }
        });
    }

    /// Path to the tests/sample-data directory (at repo root). Call `ensure_sample_data()` first if needed.
    pub fn sample_data_dir() -> std::path::PathBuf {
        ensure_sample_data();
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("tests/sample-data")
    }
}

#[derive(Clone)]
pub struct OpenOptions {
    pub delimiter: Option<u8>,
    pub has_header: Option<bool>,
    pub skip_lines: Option<usize>,
    pub skip_rows: Option<usize>,
    pub compression: Option<CompressionFormat>,
    pub pages_lookahead: Option<usize>,
    pub pages_lookback: Option<usize>,
    pub max_buffered_rows: Option<usize>,
    pub max_buffered_mb: Option<usize>,
    pub row_numbers: bool,
    pub row_start_index: usize,
    /// When true, use hive load path for directory/glob; single file uses normal load.
    pub hive: bool,
    /// When true, CSV reader tries to parse string columns as dates (e.g. YYYY-MM-DD, ISO datetime).
    pub parse_dates: bool,
    /// When true, decompress compressed CSV into memory (eager read). When false (default), decompress to a temp file and use lazy scan.
    pub decompress_in_memory: bool,
    /// Directory for decompression temp files. None = system default (e.g. TMPDIR).
    pub temp_dir: Option<std::path::PathBuf>,
    /// Excel sheet: 0-based index or sheet name (CLI only).
    pub excel_sheet: Option<String>,
    /// S3/compatible overrides (env + CLI). Take precedence over config when building CloudOptions.
    pub s3_endpoint_url_override: Option<String>,
    pub s3_access_key_id_override: Option<String>,
    pub s3_secret_access_key_override: Option<String>,
    pub s3_region_override: Option<String>,
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
            max_buffered_rows: None,
            max_buffered_mb: None,
            row_numbers: false,
            row_start_index: 1,
            hive: false,
            parse_dates: true,
            decompress_in_memory: false,
            temp_dir: None,
            excel_sheet: None,
            s3_endpoint_url_override: None,
            s3_access_key_id_override: None,
            s3_secret_access_key_override: None,
            s3_region_override: None,
        }
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions {
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

        // Compression: CLI only (auto-detect from extension when not specified)
        opts.compression = args.compression;

        // Display options: CLI args override config
        opts.pages_lookahead = args
            .pages_lookahead
            .or(Some(config.display.pages_lookahead));
        opts.pages_lookback = args.pages_lookback.or(Some(config.display.pages_lookback));
        opts.max_buffered_rows = Some(config.display.max_buffered_rows);
        opts.max_buffered_mb = Some(config.display.max_buffered_mb);

        // Row numbers: CLI flag overrides config
        opts.row_numbers = args.row_numbers || config.display.row_numbers;

        // Row start index: CLI arg overrides config
        opts.row_start_index = args
            .row_start_index
            .unwrap_or(config.display.row_start_index);

        // Hive partitioning: CLI only (no config option yet)
        opts.hive = args.hive;

        // CSV date inference: CLI overrides config; default true
        opts.parse_dates = args
            .parse_dates
            .or(config.file_loading.parse_dates)
            .unwrap_or(true);

        // Decompress-in-memory: CLI overrides config; default false (decompress to temp, use scan)
        opts.decompress_in_memory = args
            .decompress_in_memory
            .or(config.file_loading.decompress_in_memory)
            .unwrap_or(false);

        // Temp directory for decompression: CLI overrides config; default None (system temp)
        opts.temp_dir = args.temp_dir.clone().or_else(|| {
            config
                .file_loading
                .temp_dir
                .as_ref()
                .map(std::path::PathBuf::from)
        });

        // Excel sheet (CLI only)
        opts.excel_sheet = args.excel_sheet.clone();

        // S3/compatible overrides: env then CLI (CLI wins). Env vars match AWS SDK (AWS_ENDPOINT_URL, etc.)
        opts.s3_endpoint_url_override = args
            .s3_endpoint_url
            .clone()
            .or_else(|| std::env::var("AWS_ENDPOINT_URL_S3").ok())
            .or_else(|| std::env::var("AWS_ENDPOINT_URL").ok());
        opts.s3_access_key_id_override = args
            .s3_access_key_id
            .clone()
            .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok());
        opts.s3_secret_access_key_override = args
            .s3_secret_access_key
            .clone()
            .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok());
        opts.s3_region_override = args
            .s3_region
            .clone()
            .or_else(|| std::env::var("AWS_REGION").ok())
            .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok());

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
    Open(Vec<PathBuf>, OpenOptions),
    /// Open with an existing LazyFrame (e.g. from Python binding); no file load.
    OpenLazyFrame(Box<LazyFrame>, OpenOptions),
    DoLoad(Vec<PathBuf>, OpenOptions), // Internal event to actually perform loading after UI update
    /// Scan paths and build LazyFrame; then emit DoLoadSchema (phased loading).
    DoLoadScanPaths(Vec<PathBuf>, OpenOptions),
    /// Perform HTTP download (next loop so "Downloading" can render first). Then emit DoLoadFromHttpTemp.
    #[cfg(feature = "http")]
    DoDownloadHttp(String, OpenOptions),
    /// Perform S3 download to temp (next loop so "Downloading" can render first). Then emit DoLoadFromHttpTemp.
    #[cfg(feature = "cloud")]
    DoDownloadS3ToTemp(String, OpenOptions),
    /// Perform GCS download to temp (next loop so "Downloading" can render first). Then emit DoLoadFromHttpTemp.
    #[cfg(feature = "cloud")]
    DoDownloadGcsToTemp(String, OpenOptions),
    /// HTTP, S3, or GCS download finished; temp path is ready. Scan it and continue load.
    #[cfg(any(feature = "http", feature = "cloud"))]
    DoLoadFromHttpTemp(PathBuf, OpenOptions),
    /// Update phase to "Caching schema" and emit DoLoadSchemaBlocking so UI can draw before blocking.
    DoLoadSchema(Box<LazyFrame>, Option<PathBuf>, OpenOptions),
    /// Actually run collect_schema() and create state; then emit DoLoadBuffer (phased loading).
    DoLoadSchemaBlocking(Box<LazyFrame>, Option<PathBuf>, OpenOptions),
    /// First collect() on state; then emit Collect (phased loading).
    DoLoadBuffer,
    DoDecompress(Vec<PathBuf>, OpenOptions), // Internal event to perform decompression after UI shows "Decompressing"
    DoExport(PathBuf, ExportFormat, ExportOptions), // Internal event to perform export after UI shows progress
    DoExportCollect(PathBuf, ExportFormat, ExportOptions), // Collect data for export; then emit DoExportWrite
    DoExportWrite(PathBuf, ExportFormat, ExportOptions),   // Write collected DataFrame to file
    DoLoadParquetMetadata, // Load Parquet metadata when info panel is opened (deferred from render)
    Exit,
    Crash(String),
    Search(String),
    Filter(Vec<FilterStatement>),
    Sort(Vec<String>, bool),         // Columns, Ascending
    ColumnOrder(Vec<String>, usize), // Column order, locked columns count
    Pivot(PivotSpec),
    Melt(MeltSpec),
    Export(PathBuf, ExportFormat, ExportOptions), // Path, format, options
    ChartExport(PathBuf, ChartExportFormat, String), // Chart export: path, format, optional title
    DoChartExport(PathBuf, ChartExportFormat, String), // Deferred: show progress bar then run chart export
    Collect,
    Update,
    Reset,
    Resize(u16, u16), // resized (width, height)
    DoScrollDown,     // Deferred scroll: perform page_down after one frame (throbber)
    DoScrollUp,       // Deferred scroll: perform page_up
    DoScrollNext,     // Deferred scroll: perform select_next (one row down)
    DoScrollPrev,     // Deferred scroll: perform select_previous (one row up)
    DoScrollEnd,      // Deferred scroll: jump to last page (throbber)
    DoScrollHalfDown, // Deferred scroll: half page down
    DoScrollHalfUp,   // Deferred scroll: half page up
    GoToLine(usize),  // Deferred: jump to line number (when collect needed)
    /// Run the next chunk of analysis (describe/distribution); drives per-column progress.
    AnalysisChunk,
    /// Run distribution analysis (deferred so progress overlay can show first).
    AnalysisDistributionCompute,
    /// Run correlation matrix (deferred so progress overlay can show first).
    AnalysisCorrelationCompute,
}

/// Input for the shared run loop: open from file paths or from an existing LazyFrame (e.g. Python binding).
#[derive(Clone)]
pub enum RunInput {
    Paths(Vec<PathBuf>, OpenOptions),
    LazyFrame(Box<LazyFrame>, OpenOptions),
}

#[derive(Debug, Clone)]
pub struct ExportOptions {
    pub csv_delimiter: u8,
    pub csv_include_header: bool,
    pub csv_compression: Option<CompressionFormat>,
    pub json_compression: Option<CompressionFormat>,
    pub ndjson_compression: Option<CompressionFormat>,
    pub parquet_compression: Option<CompressionFormat>, // Not used in UI, but kept for API compatibility
}

#[derive(Debug, Default, PartialEq, Eq)]
pub enum InputMode {
    #[default]
    Normal,
    SortFilter,
    PivotMelt,
    Editing,
    Export,
    Info,
    Chart,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputType {
    Search,
    Filter,
    GoToLine,
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

#[derive(Default)]
pub struct SuccessModal {
    pub active: bool,
    pub message: String,
}

impl SuccessModal {
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

#[derive(Default)]
pub struct ConfirmationModal {
    pub active: bool,
    pub message: String,
    pub focus_yes: bool, // true = Yes focused, false = No focused
}

impl ConfirmationModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn show(&mut self, message: String) {
        self.active = true;
        self.message = message;
        self.focus_yes = true; // Default to Yes
    }

    pub fn hide(&mut self) {
        self.active = false;
        self.message.clear();
        self.focus_yes = true;
    }
}

#[derive(Clone, Debug, Default)]
pub enum LoadingState {
    #[default]
    Idle,
    Loading {
        /// None when loading from LazyFrame (e.g. Python binding); Some for file paths.
        file_path: Option<PathBuf>,
        file_size: u64,        // Size of compressed file in bytes (0 when no path)
        current_phase: String, // e.g., "Scanning input", "Caching schema", "Loading buffer"
        progress_percent: u16, // 0-100
    },
    Exporting {
        file_path: PathBuf,
        current_phase: String, // e.g., "Collecting data", "Writing file", "Compressing"
        progress_percent: u16, // 0-100
    },
}

impl LoadingState {
    pub fn is_loading(&self) -> bool {
        matches!(
            self,
            LoadingState::Loading { .. } | LoadingState::Exporting { .. }
        )
    }
}

/// In-progress analysis computation state (orchestration in App; modal only displays progress).
#[allow(dead_code)]
struct AnalysisComputationState {
    df: Option<DataFrame>,
    schema: Option<Arc<Schema>>,
    partial_stats: Vec<crate::statistics::ColumnStatistics>,
    current: usize,
    total: usize,
    total_rows: usize,
    sample_seed: u64,
    sample_size: Option<usize>,
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

#[derive(Default)]
struct ChartCache {
    xy: Option<ChartCacheXY>,
    x_range: Option<ChartCacheXRange>,
    histogram: Option<ChartCacheHistogram>,
    box_plot: Option<ChartCacheBoxPlot>,
    kde: Option<ChartCacheKde>,
    heatmap: Option<ChartCacheHeatmap>,
}

impl ChartCache {
    fn clear(&mut self) {
        *self = Self::default();
    }
}

struct ChartCacheXY {
    x_column: String,
    y_columns: Vec<String>,
    row_limit: Option<usize>,
    series: Vec<Vec<(f64, f64)>>,
    /// Log-scaled series when log_scale was requested; filled on first use to avoid per-frame clone.
    series_log: Option<Vec<Vec<(f64, f64)>>>,
    x_axis_kind: chart_data::XAxisTemporalKind,
}

struct ChartCacheXRange {
    x_column: String,
    row_limit: Option<usize>,
    x_min: f64,
    x_max: f64,
    x_axis_kind: chart_data::XAxisTemporalKind,
}

struct ChartCacheHistogram {
    column: String,
    bins: usize,
    row_limit: Option<usize>,
    data: chart_data::HistogramData,
}

struct ChartCacheBoxPlot {
    column: String,
    row_limit: Option<usize>,
    data: chart_data::BoxPlotData,
}

struct ChartCacheKde {
    column: String,
    bandwidth_factor: f64,
    row_limit: Option<usize>,
    data: chart_data::KdeData,
}

struct ChartCacheHeatmap {
    x_column: String,
    y_column: String,
    bins: usize,
    row_limit: Option<usize>,
    data: chart_data::HeatmapData,
}

pub struct App {
    pub data_table_state: Option<DataTableState>,
    path: Option<PathBuf>,
    original_file_format: Option<ExportFormat>, // Track original file format for default export
    original_file_delimiter: Option<u8>, // Track original file delimiter for CSV export default
    events: Sender<AppEvent>,
    focus: u32,
    debug: DebugState,
    info_modal: InfoModal,
    parquet_metadata_cache: Option<ParquetMetadataCache>,
    query_input: TextInput, // Query input widget with history support
    pub input_mode: InputMode,
    input_type: Option<InputType>,
    pub sort_filter_modal: SortFilterModal,
    pub pivot_melt_modal: PivotMeltModal,
    pub template_modal: TemplateModal,
    pub analysis_modal: AnalysisModal,
    pub chart_modal: ChartModal,
    pub chart_export_modal: ChartExportModal,
    pub export_modal: ExportModal,
    chart_cache: ChartCache,
    error_modal: ErrorModal,
    success_modal: SuccessModal,
    confirmation_modal: ConfirmationModal,
    pending_export: Option<(PathBuf, ExportFormat, ExportOptions)>, // Store export request while waiting for confirmation
    /// Collected DataFrame between DoExportCollect and DoExportWrite (two-phase export progress).
    export_df: Option<DataFrame>,
    pending_chart_export: Option<(PathBuf, ChartExportFormat, String)>,
    show_help: bool,
    help_scroll: usize, // Scroll position for help content
    cache: CacheManager,
    template_manager: TemplateManager,
    active_template_id: Option<String>, // ID of currently applied template
    loading_state: LoadingState,        // Current loading state for progress indication
    theme: Theme,                       // Color theme for UI rendering
    sampling_threshold: Option<usize>, // None = no sampling (full data); Some(n) = sample when rows >= n
    history_limit: usize, // History limit for all text inputs (from config.query.history_limit)
    table_cell_padding: u16, // Spaces between columns (from config.display.table_cell_padding)
    column_colors: bool, // When true, colorize table cells by column type (from config.display.column_colors)
    busy: bool,          // When true, show throbber and ignore keys
    throbber_frame: u8,  // Spinner frame index (0..3) for control bar
    drain_keys_on_next_loop: bool, // Main loop drains crossterm key buffer when true
    analysis_computation: Option<AnalysisComputationState>,
    app_config: AppConfig,
    /// Temp file path for HTTP-downloaded data; removed when user opens different data or exits.
    #[cfg(feature = "http")]
    http_temp_path: Option<PathBuf>,
}

impl App {
    /// Returns true when the main loop should drain the crossterm key buffer after render.
    pub fn should_drain_keys(&self) -> bool {
        self.drain_keys_on_next_loop
    }

    /// Clears the drain-keys request after the main loop has drained the buffer.
    pub fn clear_drain_keys_request(&mut self) {
        self.drain_keys_on_next_loop = false;
    }

    pub fn send_event(&mut self, event: AppEvent) -> Result<()> {
        self.events.send(event)?;
        Ok(())
    }

    /// Set loading state and phase so the progress dialog is visible. Used by run() to show
    /// loading UI immediately when launching from LazyFrame (e.g. Python) before sending the open event.
    pub fn set_loading_phase(&mut self, phase: impl Into<String>, progress_percent: u16) {
        self.busy = true;
        self.loading_state = LoadingState::Loading {
            file_path: None,
            file_size: 0,
            current_phase: phase.into(),
            progress_percent,
        };
    }

    /// Ensures file path has an extension when user did not provide one; only adds
    /// compression suffix (e.g. .gz) when compression is selected. If the user
    /// provided a path with an extension (e.g. foo.feather), that extension is kept.
    fn ensure_file_extension(
        path: &Path,
        format: ExportFormat,
        compression: Option<CompressionFormat>,
    ) -> PathBuf {
        let current_ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        let mut new_path = path.to_path_buf();

        if current_ext.is_empty() {
            // No extension: use default for format (and add compression if selected)
            let desired_ext = if let Some(comp) = compression {
                format!("{}.{}", format.extension(), comp.extension())
            } else {
                format.extension().to_string()
            };
            new_path.set_extension(&desired_ext);
        } else {
            // User provided an extension: keep it. Only add compression suffix when compression is selected.
            let is_compression_only = matches!(
                current_ext.to_lowercase().as_str(),
                "gz" | "zst" | "bz2" | "xz"
            ) && ExportFormat::from_extension(current_ext).is_none();

            if is_compression_only {
                // Path has only compression ext (e.g. file.gz); stem may have format (file.csv.gz)
                let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
                let stem_has_format = stem
                    .split('.')
                    .next_back()
                    .and_then(ExportFormat::from_extension)
                    .is_some();
                if stem_has_format {
                    if let Some(comp) = compression {
                        if let Some(format_ext) = stem
                            .split('.')
                            .next_back()
                            .and_then(ExportFormat::from_extension)
                            .map(|f| f.extension())
                        {
                            new_path =
                                PathBuf::from(stem.rsplit_once('.').map(|x| x.0).unwrap_or(stem));
                            new_path.set_extension(format!("{}.{}", format_ext, comp.extension()));
                        }
                    }
                } else if let Some(comp) = compression {
                    new_path.set_extension(format!("{}.{}", format.extension(), comp.extension()));
                } else {
                    new_path.set_extension(format.extension());
                }
            } else if let Some(comp) = compression {
                if format.supports_compression() {
                    new_path.set_extension(format!("{}.{}", current_ext, comp.extension()));
                }
                // else: path stays as-is (e.g. foo.feather stays foo.feather)
            }
            // else: path with format extension stays as-is
        }

        new_path
    }

    /// Render loading/export progress as a popover: box with frame, gauge (25% width when area is full frame).
    /// Uses theme colors for border and gauge. For loading, call with centered_rect_loading(area).
    fn render_loading_gauge(
        loading_state: &LoadingState,
        area: Rect,
        buf: &mut Buffer,
        theme: &Theme,
    ) {
        let (title, label_text, progress_percent) = match loading_state {
            LoadingState::Loading {
                current_phase,
                progress_percent,
                ..
            } => ("Loading", current_phase.clone(), progress_percent),
            LoadingState::Exporting {
                file_path,
                current_phase,
                progress_percent,
            } => {
                let label = format!("{}: {}", current_phase, file_path.display());
                ("Exporting", label, progress_percent)
            }
            LoadingState::Idle => return,
        };

        Clear.render(area, buf);

        let border_color = theme.get("modal_border");
        let gauge_fill_color = theme.get("primary_chart_series_color");

        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .title(title)
            .border_style(Style::default().fg(border_color));

        let inner = block.inner(area);
        block.render(area, buf);

        let gauge = Gauge::default()
            .gauge_style(Style::default().fg(gauge_fill_color))
            .percent(*progress_percent)
            .label(label_text);

        gauge.render(inner, buf);
    }

    pub fn new(events: Sender<AppEvent>) -> App {
        // Create default theme for backward compatibility
        let theme = Theme::from_config(&AppConfig::default().theme).unwrap_or_else(|_| {
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
        let cache = CacheManager::new(APP_NAME).unwrap_or_else(|_| CacheManager {
            cache_dir: std::env::temp_dir().join(APP_NAME),
        });

        let config_manager = ConfigManager::new(APP_NAME).unwrap_or_else(|_| ConfigManager {
            config_dir: std::env::temp_dir().join(APP_NAME).join("config"),
        });

        let template_manager = TemplateManager::new(&config_manager).unwrap_or_else(|_| {
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

        App {
            path: None,
            data_table_state: None,
            original_file_format: None,
            original_file_delimiter: None,
            events,
            focus: 0,
            debug: DebugState::default(),
            info_modal: InfoModal::new(),
            parquet_metadata_cache: None,
            query_input: TextInput::new()
                .with_history_limit(app_config.query.history_limit)
                .with_theme(&theme)
                .with_history("query".to_string()),
            input_mode: InputMode::Normal,
            input_type: None,
            sort_filter_modal: SortFilterModal::new(),
            pivot_melt_modal: PivotMeltModal::new(),
            template_modal: TemplateModal::new(),
            analysis_modal: AnalysisModal::new(),
            chart_modal: ChartModal::new(),
            chart_export_modal: ChartExportModal::new(),
            export_modal: ExportModal::new(),
            chart_cache: ChartCache::default(),
            error_modal: ErrorModal::new(),
            success_modal: SuccessModal::new(),
            confirmation_modal: ConfirmationModal::new(),
            pending_export: None,
            export_df: None,
            pending_chart_export: None,
            show_help: false,
            help_scroll: 0,
            cache,
            template_manager,
            active_template_id: None,
            loading_state: LoadingState::Idle,
            theme,
            sampling_threshold: app_config.performance.sampling_threshold,
            history_limit: app_config.query.history_limit,
            table_cell_padding: app_config.display.table_cell_padding.min(u16::MAX as usize) as u16,
            column_colors: app_config.display.column_colors,
            busy: false,
            throbber_frame: 0,
            drain_keys_on_next_loop: false,
            analysis_computation: None,
            app_config,
            #[cfg(feature = "http")]
            http_temp_path: None,
        }
    }

    pub fn enable_debug(&mut self) {
        self.debug.enabled = true;
    }

    /// Get a color from the theme by name
    fn color(&self, name: &str) -> Color {
        self.theme.get(name)
    }

    fn load(&mut self, paths: &[PathBuf], options: &OpenOptions) -> Result<()> {
        self.parquet_metadata_cache = None;
        self.export_df = None;
        let path = &paths[0]; // Primary path for format detection and single-path logic
                              // Check for compressed CSV files (e.g., file.csv.gz, file.csv.zst, etc.) — only single-file
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
        let is_compressed_csv = paths.len() == 1 && compression.is_some() && is_csv;

        // For compressed files, decompression phase is already set in DoLoad handler
        // Now actually perform decompression and CSV reading (this is the slow part)
        if is_compressed_csv {
            // Phase: Reading data (decompressing + parsing CSV; user may see "Decompressing" until we return)
            if let LoadingState::Loading {
                file_path,
                file_size,
                ..
            } = &self.loading_state
            {
                self.loading_state = LoadingState::Loading {
                    file_path: file_path.clone(),
                    file_size: *file_size,
                    current_phase: "Reading data".to_string(),
                    progress_percent: 50,
                };
            }
            let lf = DataTableState::from_csv(path, options)?; // Already passes pages_lookahead/lookback via options

            // Phase: Building lazyframe (after decompression, before rendering)
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

            // Phased loading: set "Loading buffer" so UI can show progress; caller (DoDecompress) will send DoLoadBuffer
            if let LoadingState::Loading {
                file_path,
                file_size,
                ..
            } = &self.loading_state
            {
                self.loading_state = LoadingState::Loading {
                    file_path: file_path.clone(),
                    file_size: *file_size,
                    current_phase: "Loading buffer".to_string(),
                    progress_percent: 70,
                };
            }

            self.data_table_state = Some(lf);
            self.path = Some(path.clone());
            let original_format =
                path.file_stem()
                    .and_then(|stem| stem.to_str())
                    .and_then(|stem| {
                        if stem.ends_with(".csv") {
                            Some(ExportFormat::Csv)
                        } else {
                            None
                        }
                    });
            self.original_file_format = original_format;
            self.original_file_delimiter = Some(options.delimiter.unwrap_or(b','));
            self.sort_filter_modal = SortFilterModal::new();
            self.pivot_melt_modal = PivotMeltModal::new();
            return Ok(());
        }

        // Hive path: when --hive and single path is directory or glob (not a single file), use hive load.
        // Multiple paths or single file with --hive use the normal path below.
        if paths.len() == 1 && options.hive {
            let path_str = path.as_os_str().to_string_lossy();
            let is_single_file = path.exists()
                && path.is_file()
                && !path_str.contains('*')
                && !path_str.contains("**");
            if !is_single_file {
                // Directory or glob: only Parquet supported for hive in this implementation
                let use_parquet_hive = path.is_dir()
                    || path_str.contains(".parquet")
                    || path_str.contains("*.parquet");
                if use_parquet_hive {
                    if let LoadingState::Loading {
                        file_path,
                        file_size,
                        ..
                    } = &self.loading_state
                    {
                        self.loading_state = LoadingState::Loading {
                            file_path: file_path.clone(),
                            file_size: *file_size,
                            current_phase: "Scanning partitioned dataset".to_string(),
                            progress_percent: 60,
                        };
                    }
                    let lf = DataTableState::from_parquet_hive(
                        path,
                        options.pages_lookahead,
                        options.pages_lookback,
                        options.max_buffered_rows,
                        options.max_buffered_mb,
                        options.row_numbers,
                        options.row_start_index,
                    )?;
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
                    self.loading_state = LoadingState::Idle;
                    self.data_table_state = Some(lf);
                    self.path = Some(path.clone());
                    self.original_file_format = Some(ExportFormat::Parquet);
                    self.original_file_delimiter = None;
                    self.sort_filter_modal = SortFilterModal::new();
                    self.pivot_melt_modal = PivotMeltModal::new();
                    return Ok(());
                }
                self.loading_state = LoadingState::Idle;
                return Err(color_eyre::eyre::eyre!(
                    "With --hive use a directory or a glob pattern for Parquet (e.g. path/to/dir or path/**/*.parquet)"
                ));
            }
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

        // Determine and store original file format (from first path)
        let original_format = path.extension().and_then(|e| e.to_str()).and_then(|ext| {
            if ext.eq_ignore_ascii_case("parquet") {
                Some(ExportFormat::Parquet)
            } else if ext.eq_ignore_ascii_case("csv") {
                Some(ExportFormat::Csv)
            } else if ext.eq_ignore_ascii_case("json") {
                Some(ExportFormat::Json)
            } else if ext.eq_ignore_ascii_case("jsonl") || ext.eq_ignore_ascii_case("ndjson") {
                Some(ExportFormat::Ndjson)
            } else if ext.eq_ignore_ascii_case("arrow")
                || ext.eq_ignore_ascii_case("ipc")
                || ext.eq_ignore_ascii_case("feather")
            {
                Some(ExportFormat::Ipc)
            } else if ext.eq_ignore_ascii_case("avro") {
                Some(ExportFormat::Avro)
            } else {
                None
            }
        });

        let lf = if paths.len() > 1 {
            // Multiple files: same format assumed (from first path), concatenated into one LazyFrame
            match path.extension() {
                Some(ext) if ext.eq_ignore_ascii_case("parquet") => {
                    DataTableState::from_parquet_paths(
                        paths,
                        options.pages_lookahead,
                        options.pages_lookback,
                        options.max_buffered_rows,
                        options.max_buffered_mb,
                        options.row_numbers,
                        options.row_start_index,
                    )?
                }
                Some(ext) if ext.eq_ignore_ascii_case("csv") => {
                    DataTableState::from_csv_paths(paths, options)?
                }
                Some(ext) if ext.eq_ignore_ascii_case("json") => DataTableState::from_json_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext) if ext.eq_ignore_ascii_case("jsonl") => {
                    DataTableState::from_json_lines_paths(
                        paths,
                        options.pages_lookahead,
                        options.pages_lookback,
                        options.max_buffered_rows,
                        options.max_buffered_mb,
                        options.row_numbers,
                        options.row_start_index,
                    )?
                }
                Some(ext) if ext.eq_ignore_ascii_case("ndjson") => {
                    DataTableState::from_ndjson_paths(
                        paths,
                        options.pages_lookahead,
                        options.pages_lookback,
                        options.max_buffered_rows,
                        options.max_buffered_mb,
                        options.row_numbers,
                        options.row_start_index,
                    )?
                }
                Some(ext)
                    if ext.eq_ignore_ascii_case("arrow")
                        || ext.eq_ignore_ascii_case("ipc")
                        || ext.eq_ignore_ascii_case("feather") =>
                {
                    DataTableState::from_ipc_paths(
                        paths,
                        options.pages_lookahead,
                        options.pages_lookback,
                        options.max_buffered_rows,
                        options.max_buffered_mb,
                        options.row_numbers,
                        options.row_start_index,
                    )?
                }
                Some(ext) if ext.eq_ignore_ascii_case("avro") => DataTableState::from_avro_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext) if ext.eq_ignore_ascii_case("orc") => DataTableState::from_orc_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                _ => {
                    self.loading_state = LoadingState::Idle;
                    if !paths.is_empty() && !path.exists() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("File not found: {}", path.display()),
                        )
                        .into());
                    }
                    return Err(color_eyre::eyre::eyre!(
                        "Unsupported file type for multiple files (parquet, csv, json, jsonl, ndjson, arrow/ipc/feather, avro, orc only)"
                    ));
                }
            }
        } else {
            match path.extension() {
                Some(ext) if ext.eq_ignore_ascii_case("parquet") => DataTableState::from_parquet(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
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
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext) if ext.eq_ignore_ascii_case("psv") => DataTableState::from_delimited(
                    path,
                    b'|',
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext) if ext.eq_ignore_ascii_case("json") => DataTableState::from_json(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext) if ext.eq_ignore_ascii_case("jsonl") => DataTableState::from_json_lines(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext) if ext.eq_ignore_ascii_case("ndjson") => DataTableState::from_ndjson(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext)
                    if ext.eq_ignore_ascii_case("arrow")
                        || ext.eq_ignore_ascii_case("ipc")
                        || ext.eq_ignore_ascii_case("feather") =>
                {
                    DataTableState::from_ipc(
                        path,
                        options.pages_lookahead,
                        options.pages_lookback,
                        options.max_buffered_rows,
                        options.max_buffered_mb,
                        options.row_numbers,
                        options.row_start_index,
                    )?
                }
                Some(ext) if ext.eq_ignore_ascii_case("avro") => DataTableState::from_avro(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext)
                    if ext.eq_ignore_ascii_case("xls")
                        || ext.eq_ignore_ascii_case("xlsx")
                        || ext.eq_ignore_ascii_case("xlsm")
                        || ext.eq_ignore_ascii_case("xlsb") =>
                {
                    DataTableState::from_excel(
                        path,
                        options.pages_lookahead,
                        options.pages_lookback,
                        options.max_buffered_rows,
                        options.max_buffered_mb,
                        options.row_numbers,
                        options.row_start_index,
                        options.excel_sheet.as_deref(),
                    )?
                }
                Some(ext) if ext.eq_ignore_ascii_case("orc") => DataTableState::from_orc(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                _ => {
                    self.loading_state = LoadingState::Idle;
                    if paths.len() == 1 && !path.exists() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("File not found: {}", path.display()),
                        )
                        .into());
                    }
                    return Err(color_eyre::eyre::eyre!("Unsupported file type"));
                }
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
        self.path = Some(path.clone());
        self.original_file_format = original_format;
        // Store delimiter based on file type
        self.original_file_delimiter = match path.extension().and_then(|e| e.to_str()) {
            Some(ext) if ext.eq_ignore_ascii_case("csv") => {
                // For CSV, use delimiter from options or default to comma
                Some(options.delimiter.unwrap_or(b','))
            }
            Some(ext) if ext.eq_ignore_ascii_case("tsv") => Some(b'\t'),
            Some(ext) if ext.eq_ignore_ascii_case("psv") => Some(b'|'),
            _ => None, // Not a delimited file
        };
        self.sort_filter_modal = SortFilterModal::new();
        self.pivot_melt_modal = PivotMeltModal::new();
        Ok(())
    }

    #[cfg(feature = "cloud")]
    fn build_s3_cloud_options(
        cloud: &crate::config::CloudConfig,
        options: &OpenOptions,
    ) -> CloudOptions {
        let mut opts = CloudOptions::default();
        let mut configs: Vec<(AmazonS3ConfigKey, String)> = Vec::new();
        let e = options
            .s3_endpoint_url_override
            .as_ref()
            .or(cloud.s3_endpoint_url.as_ref());
        let k = options
            .s3_access_key_id_override
            .as_ref()
            .or(cloud.s3_access_key_id.as_ref());
        let s = options
            .s3_secret_access_key_override
            .as_ref()
            .or(cloud.s3_secret_access_key.as_ref());
        let r = options
            .s3_region_override
            .as_ref()
            .or(cloud.s3_region.as_ref());
        if let Some(e) = e {
            configs.push((AmazonS3ConfigKey::Endpoint, e.clone()));
        }
        if let Some(k) = k {
            configs.push((AmazonS3ConfigKey::AccessKeyId, k.clone()));
        }
        if let Some(s) = s {
            configs.push((AmazonS3ConfigKey::SecretAccessKey, s.clone()));
        }
        if let Some(r) = r {
            configs.push((AmazonS3ConfigKey::Region, r.clone()));
        }
        if !configs.is_empty() {
            opts = opts.with_aws(configs);
        }
        opts
    }

    #[cfg(feature = "http")]
    fn download_http_to_temp(
        url: &str,
        temp_dir: Option<&Path>,
        extension: Option<&str>,
    ) -> Result<PathBuf> {
        let dir = temp_dir
            .map(Path::to_path_buf)
            .unwrap_or_else(std::env::temp_dir);
        let suffix = extension
            .map(|e| format!(".{e}"))
            .unwrap_or_else(|| ".tmp".to_string());
        let mut temp = tempfile::Builder::new()
            .suffix(&suffix)
            .tempfile_in(&dir)
            .map_err(|_| color_eyre::eyre::eyre!("Could not create a temporary file."))?;
        let response = ureq::get(url)
            .timeout(std::time::Duration::from_secs(300))
            .call()
            .map_err(|e| {
                color_eyre::eyre::eyre!("Download failed. Check the URL and your connection: {}", e)
            })?;
        let status = response.status();
        if status >= 400 {
            return Err(color_eyre::eyre::eyre!(
                "Server returned {} {}. Check the URL.",
                status,
                response.status_text()
            ));
        }
        std::io::copy(&mut response.into_reader(), &mut temp)
            .map_err(|_| color_eyre::eyre::eyre!("Download failed while saving the file."))?;
        let (_file, path) = temp
            .keep()
            .map_err(|_| color_eyre::eyre::eyre!("Could not save the downloaded file."))?;
        Ok(path)
    }

    #[cfg(feature = "cloud")]
    fn download_s3_to_temp(
        s3_url: &str,
        cloud: &crate::config::CloudConfig,
        options: &OpenOptions,
    ) -> Result<PathBuf> {
        use object_store::path::Path as OsPath;
        use object_store::ObjectStore;

        let (path_part, ext) = source::url_path_extension(s3_url);
        let (bucket, key) = path_part
            .split_once('/')
            .ok_or_else(|| color_eyre::eyre::eyre!("S3 URL must be s3://bucket/key"))?;
        if key.is_empty() {
            return Err(color_eyre::eyre::eyre!(
                "S3 URL must point to an object (e.g. s3://bucket/path/file.csv)"
            ));
        }

        let mut builder = object_store::aws::AmazonS3Builder::from_env()
            .with_url(s3_url)
            .with_bucket_name(bucket);
        let e = options
            .s3_endpoint_url_override
            .as_ref()
            .or(cloud.s3_endpoint_url.as_ref());
        let k = options
            .s3_access_key_id_override
            .as_ref()
            .or(cloud.s3_access_key_id.as_ref());
        let s = options
            .s3_secret_access_key_override
            .as_ref()
            .or(cloud.s3_secret_access_key.as_ref());
        let r = options
            .s3_region_override
            .as_ref()
            .or(cloud.s3_region.as_ref());
        if let Some(e) = e {
            builder = builder.with_endpoint(e);
        }
        if let Some(k) = k {
            builder = builder.with_access_key_id(k);
        }
        if let Some(s) = s {
            builder = builder.with_secret_access_key(s);
        }
        if let Some(r) = r {
            builder = builder.with_region(r);
        }
        let store = builder
            .build()
            .map_err(|e| color_eyre::eyre::eyre!("S3 config failed: {}", e))?;

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| color_eyre::eyre::eyre!("Could not start runtime: {}", e))?;
        let path = OsPath::from(key);
        let get_result = rt.block_on(store.get(&path)).map_err(|e| {
            color_eyre::eyre::eyre!("Could not read from S3. Check credentials and URL: {}", e)
        })?;
        let bytes = rt
            .block_on(get_result.bytes())
            .map_err(|e| color_eyre::eyre::eyre!("Could not read S3 object body: {}", e))?;

        let dir = options.temp_dir.clone().unwrap_or_else(std::env::temp_dir);
        let suffix = ext
            .as_ref()
            .map(|e| format!(".{e}"))
            .unwrap_or_else(|| ".tmp".to_string());
        let mut temp = tempfile::Builder::new()
            .suffix(&suffix)
            .tempfile_in(&dir)
            .map_err(|_| color_eyre::eyre::eyre!("Could not create a temporary file."))?;
        std::io::copy(&mut std::io::Cursor::new(bytes.as_ref()), &mut temp)
            .map_err(|_| color_eyre::eyre::eyre!("Could not write downloaded file."))?;
        let (_file, path_buf) = temp
            .keep()
            .map_err(|_| color_eyre::eyre::eyre!("Could not save the downloaded file."))?;
        Ok(path_buf)
    }

    #[cfg(feature = "cloud")]
    fn download_gcs_to_temp(gs_url: &str, options: &OpenOptions) -> Result<PathBuf> {
        use object_store::path::Path as OsPath;
        use object_store::ObjectStore;

        let (path_part, ext) = source::url_path_extension(gs_url);
        let (bucket, key) = path_part
            .split_once('/')
            .ok_or_else(|| color_eyre::eyre::eyre!("GCS URL must be gs://bucket/key"))?;
        if key.is_empty() {
            return Err(color_eyre::eyre::eyre!(
                "GCS URL must point to an object (e.g. gs://bucket/path/file.csv)"
            ));
        }

        let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
            .with_url(gs_url)
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| color_eyre::eyre::eyre!("GCS config failed: {}", e))?;

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| color_eyre::eyre::eyre!("Could not start runtime: {}", e))?;
        let path = OsPath::from(key);
        let get_result = rt.block_on(store.get(&path)).map_err(|e| {
            color_eyre::eyre::eyre!("Could not read from GCS. Check credentials and URL: {}", e)
        })?;
        let bytes = rt
            .block_on(get_result.bytes())
            .map_err(|e| color_eyre::eyre::eyre!("Could not read GCS object body: {}", e))?;

        let dir = options.temp_dir.clone().unwrap_or_else(std::env::temp_dir);
        let suffix = ext
            .as_ref()
            .map(|e| format!(".{e}"))
            .unwrap_or_else(|| ".tmp".to_string());
        let mut temp = tempfile::Builder::new()
            .suffix(&suffix)
            .tempfile_in(&dir)
            .map_err(|_| color_eyre::eyre::eyre!("Could not create a temporary file."))?;
        std::io::copy(&mut std::io::Cursor::new(bytes.as_ref()), &mut temp)
            .map_err(|_| color_eyre::eyre::eyre!("Could not write downloaded file."))?;
        let (_file, path_buf) = temp
            .keep()
            .map_err(|_| color_eyre::eyre::eyre!("Could not save the downloaded file."))?;
        Ok(path_buf)
    }

    /// Build LazyFrame from paths for phased loading (non-compressed only). Caller must not use for compressed CSV.
    fn build_lazyframe_from_paths(
        &mut self,
        paths: &[PathBuf],
        options: &OpenOptions,
    ) -> Result<LazyFrame> {
        let path = &paths[0];
        match source::input_source(path) {
            source::InputSource::Http(_url) => {
                #[cfg(feature = "http")]
                {
                    return Err(color_eyre::eyre::eyre!(
                        "HTTP/HTTPS load is handled in the event loop; this path should not be reached."
                    ));
                }
                #[cfg(not(feature = "http"))]
                {
                    return Err(color_eyre::eyre::eyre!(
                        "HTTP/HTTPS URLs are not supported in this build. Rebuild with default features."
                    ));
                }
            }
            source::InputSource::S3(url) => {
                #[cfg(feature = "cloud")]
                {
                    let full = format!("s3://{url}");
                    let (_, ext) = source::url_path_extension(&full);
                    let is_parquet = ext
                        .as_ref()
                        .map(|e| e.eq_ignore_ascii_case("parquet"))
                        .unwrap_or(false);
                    if !is_parquet {
                        return Err(color_eyre::eyre::eyre!(
                            "S3 non-Parquet is handled in the event loop (download to temp); this path should not be reached."
                        ));
                    }
                    let cloud_opts = Self::build_s3_cloud_options(&self.app_config.cloud, options);
                    let pl_path = PlPathRef::new(&full).into_owned();
                    let is_glob = full.contains('*') || full.ends_with('/');
                    let hive_options = if is_glob {
                        polars::io::HiveOptions::new_enabled()
                    } else {
                        polars::io::HiveOptions::default()
                    };
                    let args = ScanArgsParquet {
                        cloud_options: Some(cloud_opts),
                        hive_options,
                        glob: is_glob,
                        ..Default::default()
                    };
                    let lf = LazyFrame::scan_parquet(pl_path, args).map_err(|e| {
                        color_eyre::eyre::eyre!(
                            "Could not read from S3. Check credentials and URL: {}",
                            e
                        )
                    })?;
                    let state = DataTableState::from_lazyframe(lf, options)?;
                    return Ok(state.lf);
                }
                #[cfg(not(feature = "cloud"))]
                {
                    return Err(color_eyre::eyre::eyre!(
                        "S3 is not supported in this build. Rebuild with default features and set AWS credentials (e.g. AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)."
                    ));
                }
            }
            source::InputSource::Gcs(url) => {
                #[cfg(feature = "cloud")]
                {
                    let full = format!("gs://{url}");
                    let (_, ext) = source::url_path_extension(&full);
                    let is_parquet = ext
                        .as_ref()
                        .map(|e| e.eq_ignore_ascii_case("parquet"))
                        .unwrap_or(false);
                    if !is_parquet {
                        return Err(color_eyre::eyre::eyre!(
                            "GCS non-Parquet is handled in the event loop (download to temp); this path should not be reached."
                        ));
                    }
                    let pl_path = PlPathRef::new(&full).into_owned();
                    let is_glob = full.contains('*') || full.ends_with('/');
                    let hive_options = if is_glob {
                        polars::io::HiveOptions::new_enabled()
                    } else {
                        polars::io::HiveOptions::default()
                    };
                    let args = ScanArgsParquet {
                        cloud_options: Some(CloudOptions::default()),
                        hive_options,
                        glob: is_glob,
                        ..Default::default()
                    };
                    let lf = LazyFrame::scan_parquet(pl_path, args).map_err(|e| {
                        color_eyre::eyre::eyre!(
                            "Could not read from GCS. Check credentials and URL: {}",
                            e
                        )
                    })?;
                    let state = DataTableState::from_lazyframe(lf, options)?;
                    return Ok(state.lf);
                }
                #[cfg(not(feature = "cloud"))]
                {
                    return Err(color_eyre::eyre::eyre!(
                        "GCS (gs://) is not supported in this build. Rebuild with default features."
                    ));
                }
            }
            source::InputSource::Local(_) => {}
        }

        if paths.len() == 1 && options.hive {
            let path_str = path.as_os_str().to_string_lossy();
            let is_single_file = path.exists()
                && path.is_file()
                && !path_str.contains('*')
                && !path_str.contains("**");
            if !is_single_file {
                let use_parquet_hive = path.is_dir()
                    || path_str.contains(".parquet")
                    || path_str.contains("*.parquet");
                if use_parquet_hive {
                    // Only build LazyFrame here; schema + partition discovery happen in DoLoadSchema ("Caching schema")
                    return DataTableState::scan_parquet_hive(path);
                }
                return Err(color_eyre::eyre::eyre!(
                    "With --hive use a directory or a glob pattern for Parquet (e.g. path/to/dir or path/**/*.parquet)"
                ));
            }
        }

        let lf = if paths.len() > 1 {
            match path.extension() {
                Some(ext) if ext.eq_ignore_ascii_case("parquet") => {
                    DataTableState::from_parquet_paths(
                        paths,
                        options.pages_lookahead,
                        options.pages_lookback,
                        options.max_buffered_rows,
                        options.max_buffered_mb,
                        options.row_numbers,
                        options.row_start_index,
                    )?
                }
                Some(ext) if ext.eq_ignore_ascii_case("csv") => {
                    DataTableState::from_csv_paths(paths, options)?
                }
                Some(ext) if ext.eq_ignore_ascii_case("json") => DataTableState::from_json_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext) if ext.eq_ignore_ascii_case("jsonl") => {
                    DataTableState::from_json_lines_paths(
                        paths,
                        options.pages_lookahead,
                        options.pages_lookback,
                        options.max_buffered_rows,
                        options.max_buffered_mb,
                        options.row_numbers,
                        options.row_start_index,
                    )?
                }
                Some(ext) if ext.eq_ignore_ascii_case("ndjson") => {
                    DataTableState::from_ndjson_paths(
                        paths,
                        options.pages_lookahead,
                        options.pages_lookback,
                        options.max_buffered_rows,
                        options.max_buffered_mb,
                        options.row_numbers,
                        options.row_start_index,
                    )?
                }
                Some(ext)
                    if ext.eq_ignore_ascii_case("arrow")
                        || ext.eq_ignore_ascii_case("ipc")
                        || ext.eq_ignore_ascii_case("feather") =>
                {
                    DataTableState::from_ipc_paths(
                        paths,
                        options.pages_lookahead,
                        options.pages_lookback,
                        options.max_buffered_rows,
                        options.max_buffered_mb,
                        options.row_numbers,
                        options.row_start_index,
                    )?
                }
                Some(ext) if ext.eq_ignore_ascii_case("avro") => DataTableState::from_avro_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext) if ext.eq_ignore_ascii_case("orc") => DataTableState::from_orc_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                _ => {
                    if !paths.is_empty() && !path.exists() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("File not found: {}", path.display()),
                        )
                        .into());
                    }
                    return Err(color_eyre::eyre::eyre!(
                        "Unsupported file type for multiple files (parquet, csv, json, jsonl, ndjson, arrow/ipc/feather, avro, orc only)"
                    ));
                }
            }
        } else {
            match path.extension() {
                Some(ext) if ext.eq_ignore_ascii_case("parquet") => DataTableState::from_parquet(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext) if ext.eq_ignore_ascii_case("csv") => {
                    DataTableState::from_csv(path, options)?
                }
                Some(ext) if ext.eq_ignore_ascii_case("tsv") => DataTableState::from_delimited(
                    path,
                    b'\t',
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext) if ext.eq_ignore_ascii_case("psv") => DataTableState::from_delimited(
                    path,
                    b'|',
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext) if ext.eq_ignore_ascii_case("json") => DataTableState::from_json(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext) if ext.eq_ignore_ascii_case("jsonl") => DataTableState::from_json_lines(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext) if ext.eq_ignore_ascii_case("ndjson") => DataTableState::from_ndjson(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext)
                    if ext.eq_ignore_ascii_case("arrow")
                        || ext.eq_ignore_ascii_case("ipc")
                        || ext.eq_ignore_ascii_case("feather") =>
                {
                    DataTableState::from_ipc(
                        path,
                        options.pages_lookahead,
                        options.pages_lookback,
                        options.max_buffered_rows,
                        options.max_buffered_mb,
                        options.row_numbers,
                        options.row_start_index,
                    )?
                }
                Some(ext) if ext.eq_ignore_ascii_case("avro") => DataTableState::from_avro(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(ext)
                    if ext.eq_ignore_ascii_case("xls")
                        || ext.eq_ignore_ascii_case("xlsx")
                        || ext.eq_ignore_ascii_case("xlsm")
                        || ext.eq_ignore_ascii_case("xlsb") =>
                {
                    DataTableState::from_excel(
                        path,
                        options.pages_lookahead,
                        options.pages_lookback,
                        options.max_buffered_rows,
                        options.max_buffered_mb,
                        options.row_numbers,
                        options.row_start_index,
                        options.excel_sheet.as_deref(),
                    )?
                }
                Some(ext) if ext.eq_ignore_ascii_case("orc") => DataTableState::from_orc(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                _ => {
                    if paths.len() == 1 && !path.exists() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("File not found: {}", path.display()),
                        )
                        .into());
                    }
                    return Err(color_eyre::eyre::eyre!("Unsupported file type"));
                }
            }
        };
        Ok(lf.lf)
    }

    /// Set the appropriate help overlay visible (main, template, or analysis). No-op if already visible.
    fn open_help_overlay(&mut self) {
        let already = self.show_help
            || (self.template_modal.active && self.template_modal.show_help)
            || (self.analysis_modal.active && self.analysis_modal.show_help);
        if already {
            return;
        }
        if self.analysis_modal.active {
            self.analysis_modal.show_help = true;
        } else if self.template_modal.active {
            self.template_modal.show_help = true;
        } else {
            self.show_help = true;
        }
    }

    fn key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        self.debug.on_key(event);

        // F1 opens help first so no other branch (e.g. Editing) can consume it.
        if event.code == KeyCode::F(1) {
            self.open_help_overlay();
            return None;
        }

        // Handle modals first - they have highest priority
        // Confirmation modal (for overwrite)
        if self.confirmation_modal.active {
            match event.code {
                KeyCode::Left | KeyCode::Char('h') => {
                    self.confirmation_modal.focus_yes = true;
                }
                KeyCode::Right | KeyCode::Char('l') => {
                    self.confirmation_modal.focus_yes = false;
                }
                KeyCode::Tab => {
                    // Toggle between Yes and No
                    self.confirmation_modal.focus_yes = !self.confirmation_modal.focus_yes;
                }
                KeyCode::Enter => {
                    if self.confirmation_modal.focus_yes {
                        // User confirmed overwrite: chart export first, then dataframe export
                        if let Some((path, format, title)) = self.pending_chart_export.take() {
                            self.confirmation_modal.hide();
                            return Some(AppEvent::ChartExport(path, format, title));
                        }
                        if let Some((path, format, options)) = self.pending_export.take() {
                            self.confirmation_modal.hide();
                            return Some(AppEvent::Export(path, format, options));
                        }
                    } else {
                        // User cancelled: if chart export overwrite, reopen chart export modal with path pre-filled
                        if let Some((path, format, _)) = self.pending_chart_export.take() {
                            self.chart_export_modal.reopen_with_path(&path, format);
                        }
                        self.pending_export = None;
                        self.confirmation_modal.hide();
                    }
                }
                KeyCode::Esc => {
                    // Cancel: if chart export overwrite, reopen chart export modal with path pre-filled
                    if let Some((path, format, _)) = self.pending_chart_export.take() {
                        self.chart_export_modal.reopen_with_path(&path, format);
                    }
                    self.pending_export = None;
                    self.confirmation_modal.hide();
                }
                _ => {}
            }
            return None;
        }
        // Success modal
        if self.success_modal.active {
            match event.code {
                KeyCode::Esc | KeyCode::Enter => {
                    self.success_modal.hide();
                }
                _ => {}
            }
            return None;
        }
        // Error modal
        if self.error_modal.active {
            match event.code {
                KeyCode::Esc | KeyCode::Enter => {
                    self.error_modal.hide();
                }
                _ => {}
            }
            return None;
        }

        // Main table: left/right scroll columns (before help/mode blocks so column scroll always works in Normal).
        // No is_press()/is_release() check: some terminals do not report key kind correctly.
        // Exclude template/analysis modals so they can handle Left/Right themselves.
        let in_main_table = !(self.input_mode != InputMode::Normal
            || self.show_help
            || self.template_modal.active
            || self.analysis_modal.active);
        if in_main_table {
            let did_scroll = match event.code {
                KeyCode::Right | KeyCode::Char('l') => {
                    if let Some(ref mut state) = self.data_table_state {
                        state.scroll_right();
                        if self.debug.enabled {
                            self.debug.last_action = "scroll_right".to_string();
                        }
                        true
                    } else {
                        false
                    }
                }
                KeyCode::Left | KeyCode::Char('h') => {
                    if let Some(ref mut state) = self.data_table_state {
                        state.scroll_left();
                        if self.debug.enabled {
                            self.debug.last_action = "scroll_left".to_string();
                        }
                        true
                    } else {
                        false
                    }
                }
                _ => false,
            };
            if did_scroll {
                return None;
            }
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
                KeyCode::Char('?') => {
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

        if event.code == KeyCode::Char('?') {
            let ctrl_help = event.modifiers.contains(KeyModifiers::CONTROL);
            let in_text_input = match self.input_mode {
                InputMode::Editing => true,
                InputMode::Export => matches!(
                    self.export_modal.focus,
                    ExportFocus::PathInput | ExportFocus::CsvDelimiter
                ),
                InputMode::SortFilter => {
                    let on_body = self.sort_filter_modal.focus == SortFilterFocus::Body;
                    let filter_tab = self.sort_filter_modal.active_tab == SortFilterTab::Filter;
                    on_body
                        && filter_tab
                        && self.sort_filter_modal.filter.focus == FilterFocus::Value
                }
                InputMode::PivotMelt => matches!(
                    self.pivot_melt_modal.focus,
                    PivotMeltFocus::PivotFilter
                        | PivotMeltFocus::MeltFilter
                        | PivotMeltFocus::MeltPattern
                        | PivotMeltFocus::MeltVarName
                        | PivotMeltFocus::MeltValName
                ),
                InputMode::Info | InputMode::Chart => false,
                InputMode::Normal => {
                    if self.template_modal.active
                        && self.template_modal.mode != TemplateModalMode::List
                    {
                        matches!(
                            self.template_modal.create_focus,
                            CreateFocus::Name
                                | CreateFocus::Description
                                | CreateFocus::ExactPath
                                | CreateFocus::RelativePath
                                | CreateFocus::PathPattern
                                | CreateFocus::FilenamePattern
                        )
                    } else {
                        false
                    }
                }
            };
            // Ctrl-? always opens help; bare ? only when not in a text field
            if ctrl_help || !in_text_input {
                self.open_help_overlay();
                return None;
            }
        }

        if self.input_mode == InputMode::SortFilter {
            let on_tab_bar = self.sort_filter_modal.focus == SortFilterFocus::TabBar;
            let on_body = self.sort_filter_modal.focus == SortFilterFocus::Body;
            let on_apply = self.sort_filter_modal.focus == SortFilterFocus::Apply;
            let on_cancel = self.sort_filter_modal.focus == SortFilterFocus::Cancel;
            let on_clear = self.sort_filter_modal.focus == SortFilterFocus::Clear;
            let sort_tab = self.sort_filter_modal.active_tab == SortFilterTab::Sort;
            let filter_tab = self.sort_filter_modal.active_tab == SortFilterTab::Filter;

            match event.code {
                KeyCode::Esc => {
                    for col in &mut self.sort_filter_modal.sort.columns {
                        col.is_to_be_locked = false;
                    }
                    self.sort_filter_modal.sort.has_unapplied_changes = false;
                    self.sort_filter_modal.close();
                    self.input_mode = InputMode::Normal;
                }
                KeyCode::Tab => self.sort_filter_modal.next_focus(),
                KeyCode::BackTab => self.sort_filter_modal.prev_focus(),
                KeyCode::Left | KeyCode::Char('h') if on_tab_bar => {
                    self.sort_filter_modal.switch_tab();
                }
                KeyCode::Right | KeyCode::Char('l') if on_tab_bar => {
                    self.sort_filter_modal.switch_tab();
                }
                KeyCode::Enter if event.modifiers.contains(KeyModifiers::CONTROL) && sort_tab => {
                    let columns = self.sort_filter_modal.sort.get_sorted_columns();
                    let column_order = self.sort_filter_modal.sort.get_column_order();
                    let locked_count = self.sort_filter_modal.sort.get_locked_columns_count();
                    let ascending = self.sort_filter_modal.sort.ascending;
                    self.sort_filter_modal.sort.has_unapplied_changes = false;
                    self.sort_filter_modal.close();
                    self.input_mode = InputMode::Normal;
                    let _ = self.send_event(AppEvent::ColumnOrder(column_order, locked_count));
                    return Some(AppEvent::Sort(columns, ascending));
                }
                KeyCode::Enter if on_apply => {
                    if sort_tab {
                        let columns = self.sort_filter_modal.sort.get_sorted_columns();
                        let column_order = self.sort_filter_modal.sort.get_column_order();
                        let locked_count = self.sort_filter_modal.sort.get_locked_columns_count();
                        let ascending = self.sort_filter_modal.sort.ascending;
                        self.sort_filter_modal.sort.has_unapplied_changes = false;
                        self.sort_filter_modal.close();
                        self.input_mode = InputMode::Normal;
                        let _ = self.send_event(AppEvent::ColumnOrder(column_order, locked_count));
                        return Some(AppEvent::Sort(columns, ascending));
                    } else {
                        let statements = self.sort_filter_modal.filter.statements.clone();
                        self.sort_filter_modal.close();
                        self.input_mode = InputMode::Normal;
                        return Some(AppEvent::Filter(statements));
                    }
                }
                KeyCode::Enter if on_cancel => {
                    for col in &mut self.sort_filter_modal.sort.columns {
                        col.is_to_be_locked = false;
                    }
                    self.sort_filter_modal.sort.has_unapplied_changes = false;
                    self.sort_filter_modal.close();
                    self.input_mode = InputMode::Normal;
                }
                KeyCode::Enter if on_clear => {
                    if sort_tab {
                        self.sort_filter_modal.sort.clear_selection();
                    } else {
                        self.sort_filter_modal.filter.statements.clear();
                        self.sort_filter_modal.filter.list_state.select(None);
                    }
                }
                KeyCode::Char(' ')
                    if on_body
                        && sort_tab
                        && self.sort_filter_modal.sort.focus == SortFocus::ColumnList =>
                {
                    self.sort_filter_modal.sort.toggle_selection();
                }
                KeyCode::Char(' ')
                    if on_body
                        && sort_tab
                        && self.sort_filter_modal.sort.focus == SortFocus::Order =>
                {
                    self.sort_filter_modal.sort.ascending = !self.sort_filter_modal.sort.ascending;
                    self.sort_filter_modal.sort.has_unapplied_changes = true;
                }
                KeyCode::Char(' ') if on_apply && sort_tab => {
                    let columns = self.sort_filter_modal.sort.get_sorted_columns();
                    let column_order = self.sort_filter_modal.sort.get_column_order();
                    let locked_count = self.sort_filter_modal.sort.get_locked_columns_count();
                    let ascending = self.sort_filter_modal.sort.ascending;
                    self.sort_filter_modal.sort.has_unapplied_changes = false;
                    let _ = self.send_event(AppEvent::ColumnOrder(column_order, locked_count));
                    return Some(AppEvent::Sort(columns, ascending));
                }
                KeyCode::Enter if on_body && filter_tab => {
                    match self.sort_filter_modal.filter.focus {
                        FilterFocus::Add => {
                            self.sort_filter_modal.filter.add_statement();
                        }
                        FilterFocus::Statements => {
                            let m = &mut self.sort_filter_modal.filter;
                            if let Some(idx) = m.list_state.selected() {
                                if idx < m.statements.len() {
                                    m.statements.remove(idx);
                                    if m.statements.is_empty() {
                                        m.list_state.select(None);
                                        m.focus = FilterFocus::Column;
                                    } else {
                                        m.list_state
                                            .select(Some(m.statements.len().saturating_sub(1)));
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                KeyCode::Enter if on_body && sort_tab => match self.sort_filter_modal.sort.focus {
                    SortFocus::Filter => {
                        self.sort_filter_modal.sort.focus = SortFocus::ColumnList;
                    }
                    SortFocus::ColumnList => {
                        self.sort_filter_modal.sort.toggle_selection();
                        let columns = self.sort_filter_modal.sort.get_sorted_columns();
                        let column_order = self.sort_filter_modal.sort.get_column_order();
                        let locked_count = self.sort_filter_modal.sort.get_locked_columns_count();
                        let ascending = self.sort_filter_modal.sort.ascending;
                        self.sort_filter_modal.sort.has_unapplied_changes = false;
                        let _ = self.send_event(AppEvent::ColumnOrder(column_order, locked_count));
                        return Some(AppEvent::Sort(columns, ascending));
                    }
                    SortFocus::Order => {
                        self.sort_filter_modal.sort.ascending =
                            !self.sort_filter_modal.sort.ascending;
                        self.sort_filter_modal.sort.has_unapplied_changes = true;
                    }
                    _ => {}
                },
                KeyCode::Down
                    if on_body
                        && filter_tab
                        && self.sort_filter_modal.filter.focus == FilterFocus::Statements =>
                {
                    let m = &mut self.sort_filter_modal.filter;
                    let i = match m.list_state.selected() {
                        Some(i) => {
                            if i >= m.statements.len().saturating_sub(1) {
                                0
                            } else {
                                i + 1
                            }
                        }
                        None => 0,
                    };
                    m.list_state.select(Some(i));
                }
                KeyCode::Up
                    if on_body
                        && filter_tab
                        && self.sort_filter_modal.filter.focus == FilterFocus::Statements =>
                {
                    let m = &mut self.sort_filter_modal.filter;
                    let i = match m.list_state.selected() {
                        Some(i) => {
                            if i == 0 {
                                m.statements.len().saturating_sub(1)
                            } else {
                                i - 1
                            }
                        }
                        None => 0,
                    };
                    m.list_state.select(Some(i));
                }
                KeyCode::Down | KeyCode::Char('j') if on_body && sort_tab => {
                    let s = &mut self.sort_filter_modal.sort;
                    if s.focus == SortFocus::ColumnList {
                        let i = match s.table_state.selected() {
                            Some(i) => {
                                if i >= s.filtered_columns().len().saturating_sub(1) {
                                    0
                                } else {
                                    i + 1
                                }
                            }
                            None => 0,
                        };
                        s.table_state.select(Some(i));
                    } else {
                        let _ = s.next_body_focus();
                    }
                }
                KeyCode::Up | KeyCode::Char('k') if on_body && sort_tab => {
                    let s = &mut self.sort_filter_modal.sort;
                    if s.focus == SortFocus::ColumnList {
                        let i = match s.table_state.selected() {
                            Some(i) => {
                                if i == 0 {
                                    s.filtered_columns().len().saturating_sub(1)
                                } else {
                                    i - 1
                                }
                            }
                            None => 0,
                        };
                        s.table_state.select(Some(i));
                    } else {
                        let _ = s.prev_body_focus();
                    }
                }
                KeyCode::Char(']')
                    if on_body
                        && sort_tab
                        && self.sort_filter_modal.sort.focus == SortFocus::ColumnList =>
                {
                    self.sort_filter_modal.sort.move_selection_down();
                }
                KeyCode::Char('[')
                    if on_body
                        && sort_tab
                        && self.sort_filter_modal.sort.focus == SortFocus::ColumnList =>
                {
                    self.sort_filter_modal.sort.move_selection_up();
                }
                KeyCode::Char('+') | KeyCode::Char('=')
                    if on_body
                        && sort_tab
                        && self.sort_filter_modal.sort.focus == SortFocus::ColumnList =>
                {
                    self.sort_filter_modal.sort.move_column_display_up();
                    self.sort_filter_modal.sort.has_unapplied_changes = true;
                }
                KeyCode::Char('-') | KeyCode::Char('_')
                    if on_body
                        && sort_tab
                        && self.sort_filter_modal.sort.focus == SortFocus::ColumnList =>
                {
                    self.sort_filter_modal.sort.move_column_display_down();
                    self.sort_filter_modal.sort.has_unapplied_changes = true;
                }
                KeyCode::Char('L')
                    if on_body
                        && sort_tab
                        && self.sort_filter_modal.sort.focus == SortFocus::ColumnList =>
                {
                    self.sort_filter_modal.sort.toggle_lock_at_column();
                    self.sort_filter_modal.sort.has_unapplied_changes = true;
                }
                KeyCode::Char('v')
                    if on_body
                        && sort_tab
                        && self.sort_filter_modal.sort.focus == SortFocus::ColumnList =>
                {
                    self.sort_filter_modal.sort.toggle_visibility();
                    self.sort_filter_modal.sort.has_unapplied_changes = true;
                }
                KeyCode::Char(c)
                    if on_body
                        && sort_tab
                        && self.sort_filter_modal.sort.focus == SortFocus::ColumnList
                        && c.is_ascii_digit() =>
                {
                    if let Some(digit) = c.to_digit(10) {
                        self.sort_filter_modal
                            .sort
                            .jump_selection_to_order(digit as usize);
                    }
                }
                // Handle filter input field in sort tab
                // Only handle keys that the text input should process
                // Special keys like Tab, Esc, Enter are handled by other patterns above
                _ if on_body
                    && sort_tab
                    && self.sort_filter_modal.sort.focus == SortFocus::Filter
                    && !matches!(
                        event.code,
                        KeyCode::Tab
                            | KeyCode::BackTab
                            | KeyCode::Esc
                            | KeyCode::Enter
                            | KeyCode::Up
                            | KeyCode::Down
                    ) =>
                {
                    // Pass key events to the filter input
                    let _ = self
                        .sort_filter_modal
                        .sort
                        .filter_input
                        .handle_key(event, Some(&self.cache));
                }
                KeyCode::Char(c)
                    if on_body
                        && filter_tab
                        && self.sort_filter_modal.filter.focus == FilterFocus::Value =>
                {
                    self.sort_filter_modal.filter.new_value.push(c);
                }
                KeyCode::Backspace
                    if on_body
                        && filter_tab
                        && self.sort_filter_modal.filter.focus == FilterFocus::Value =>
                {
                    self.sort_filter_modal.filter.new_value.pop();
                }
                KeyCode::Right | KeyCode::Char('l') if on_body && filter_tab => {
                    let m = &mut self.sort_filter_modal.filter;
                    match m.focus {
                        FilterFocus::Column => {
                            m.new_column_idx =
                                (m.new_column_idx + 1) % m.available_columns.len().max(1);
                        }
                        FilterFocus::Operator => {
                            m.new_operator_idx =
                                (m.new_operator_idx + 1) % FilterOperator::iterator().count();
                        }
                        FilterFocus::Logical => {
                            m.new_logical_idx =
                                (m.new_logical_idx + 1) % LogicalOperator::iterator().count();
                        }
                        _ => {}
                    }
                }
                KeyCode::Left | KeyCode::Char('h') if on_body && filter_tab => {
                    let m = &mut self.sort_filter_modal.filter;
                    match m.focus {
                        FilterFocus::Column => {
                            m.new_column_idx = if m.new_column_idx == 0 {
                                m.available_columns.len().saturating_sub(1)
                            } else {
                                m.new_column_idx - 1
                            };
                        }
                        FilterFocus::Operator => {
                            m.new_operator_idx = if m.new_operator_idx == 0 {
                                FilterOperator::iterator().count() - 1
                            } else {
                                m.new_operator_idx - 1
                            };
                        }
                        FilterFocus::Logical => {
                            m.new_logical_idx = if m.new_logical_idx == 0 {
                                LogicalOperator::iterator().count() - 1
                            } else {
                                m.new_logical_idx - 1
                            };
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
            return None;
        }

        if self.input_mode == InputMode::Export {
            match event.code {
                KeyCode::Esc => {
                    self.export_modal.close();
                    self.input_mode = InputMode::Normal;
                }
                KeyCode::Tab => self.export_modal.next_focus(),
                KeyCode::BackTab => self.export_modal.prev_focus(),
                KeyCode::Up | KeyCode::Char('k') => {
                    match self.export_modal.focus {
                        ExportFocus::FormatSelector => {
                            // Cycle through formats
                            let current_idx = ExportFormat::ALL
                                .iter()
                                .position(|&f| f == self.export_modal.selected_format)
                                .unwrap_or(0);
                            let prev_idx = if current_idx == 0 {
                                ExportFormat::ALL.len() - 1
                            } else {
                                current_idx - 1
                            };
                            self.export_modal.selected_format = ExportFormat::ALL[prev_idx];
                        }
                        ExportFocus::PathInput => {
                            // Pass to text input widget (for history navigation)
                            self.export_modal.path_input.handle_key(event, None);
                        }
                        ExportFocus::CsvDelimiter => {
                            // Pass to text input widget (for history navigation)
                            self.export_modal
                                .csv_delimiter_input
                                .handle_key(event, None);
                        }
                        ExportFocus::CsvCompression
                        | ExportFocus::JsonCompression
                        | ExportFocus::NdjsonCompression => {
                            // Left to move to previous compression option
                            self.export_modal.cycle_compression_backward();
                        }
                        _ => {
                            self.export_modal.prev_focus();
                        }
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    match self.export_modal.focus {
                        ExportFocus::FormatSelector => {
                            // Cycle through formats
                            let current_idx = ExportFormat::ALL
                                .iter()
                                .position(|&f| f == self.export_modal.selected_format)
                                .unwrap_or(0);
                            let next_idx = (current_idx + 1) % ExportFormat::ALL.len();
                            self.export_modal.selected_format = ExportFormat::ALL[next_idx];
                        }
                        ExportFocus::PathInput => {
                            // Pass to text input widget (for history navigation)
                            self.export_modal.path_input.handle_key(event, None);
                        }
                        ExportFocus::CsvDelimiter => {
                            // Pass to text input widget (for history navigation)
                            self.export_modal
                                .csv_delimiter_input
                                .handle_key(event, None);
                        }
                        ExportFocus::CsvCompression
                        | ExportFocus::JsonCompression
                        | ExportFocus::NdjsonCompression => {
                            // Right to move to next compression option
                            self.export_modal.cycle_compression();
                        }
                        _ => {
                            self.export_modal.next_focus();
                        }
                    }
                }
                KeyCode::Left | KeyCode::Char('h') => {
                    match self.export_modal.focus {
                        ExportFocus::PathInput => {
                            self.export_modal.path_input.handle_key(event, None);
                        }
                        ExportFocus::CsvDelimiter => {
                            self.export_modal
                                .csv_delimiter_input
                                .handle_key(event, None);
                        }
                        ExportFocus::FormatSelector => {
                            // Don't change focus in format selector
                        }
                        ExportFocus::CsvCompression
                        | ExportFocus::JsonCompression
                        | ExportFocus::NdjsonCompression => {
                            // Move to previous compression option
                            self.export_modal.cycle_compression_backward();
                        }
                        _ => self.export_modal.prev_focus(),
                    }
                }
                KeyCode::Right | KeyCode::Char('l') => {
                    match self.export_modal.focus {
                        ExportFocus::PathInput => {
                            self.export_modal.path_input.handle_key(event, None);
                        }
                        ExportFocus::CsvDelimiter => {
                            self.export_modal
                                .csv_delimiter_input
                                .handle_key(event, None);
                        }
                        ExportFocus::FormatSelector => {
                            // Don't change focus in format selector
                        }
                        ExportFocus::CsvCompression
                        | ExportFocus::JsonCompression
                        | ExportFocus::NdjsonCompression => {
                            // Move to next compression option
                            self.export_modal.cycle_compression();
                        }
                        _ => self.export_modal.next_focus(),
                    }
                }
                KeyCode::Enter => {
                    match self.export_modal.focus {
                        ExportFocus::PathInput => {
                            // Enter from path input triggers export (same as Export button)
                            let path_str = self.export_modal.path_input.value.trim();
                            if !path_str.is_empty() {
                                let mut path = PathBuf::from(path_str);
                                let format = self.export_modal.selected_format;
                                // Get compression format for this export format
                                let compression = match format {
                                    ExportFormat::Csv => self.export_modal.csv_compression,
                                    ExportFormat::Json => self.export_modal.json_compression,
                                    ExportFormat::Ndjson => self.export_modal.ndjson_compression,
                                    ExportFormat::Parquet
                                    | ExportFormat::Ipc
                                    | ExportFormat::Avro => None,
                                };
                                // Ensure file extension is present (including compression extension if needed)
                                let path_with_ext =
                                    Self::ensure_file_extension(&path, format, compression);
                                // Update the path input to show the extension
                                if path_with_ext != path {
                                    self.export_modal
                                        .path_input
                                        .set_value(path_with_ext.display().to_string());
                                }
                                path = path_with_ext;
                                let delimiter =
                                    self.export_modal
                                        .csv_delimiter_input
                                        .value
                                        .chars()
                                        .next()
                                        .unwrap_or(',') as u8;
                                let options = ExportOptions {
                                    csv_delimiter: delimiter,
                                    csv_include_header: self.export_modal.csv_include_header,
                                    csv_compression: self.export_modal.csv_compression,
                                    json_compression: self.export_modal.json_compression,
                                    ndjson_compression: self.export_modal.ndjson_compression,
                                    parquet_compression: None,
                                };
                                // Check if file exists and show confirmation
                                if path.exists() {
                                    let path_display = path.display().to_string();
                                    self.pending_export = Some((path, format, options));
                                    self.confirmation_modal.show(format!(
                                        "File already exists:\n{}\n\nDo you wish to overwrite this file?",
                                        path_display
                                    ));
                                    self.export_modal.close();
                                    self.input_mode = InputMode::Normal;
                                } else {
                                    // Start export with progress
                                    self.export_modal.close();
                                    self.input_mode = InputMode::Normal;
                                    return Some(AppEvent::Export(path, format, options));
                                }
                            }
                        }
                        ExportFocus::ExportButton => {
                            if !self.export_modal.path_input.value.is_empty() {
                                let mut path = PathBuf::from(&self.export_modal.path_input.value);
                                let format = self.export_modal.selected_format;
                                // Get compression format for this export format
                                let compression = match format {
                                    ExportFormat::Csv => self.export_modal.csv_compression,
                                    ExportFormat::Json => self.export_modal.json_compression,
                                    ExportFormat::Ndjson => self.export_modal.ndjson_compression,
                                    ExportFormat::Parquet
                                    | ExportFormat::Ipc
                                    | ExportFormat::Avro => None,
                                };
                                // Ensure file extension is present (including compression extension if needed)
                                let path_with_ext =
                                    Self::ensure_file_extension(&path, format, compression);
                                // Update the path input to show the extension
                                if path_with_ext != path {
                                    self.export_modal
                                        .path_input
                                        .set_value(path_with_ext.display().to_string());
                                }
                                path = path_with_ext;
                                let delimiter =
                                    self.export_modal
                                        .csv_delimiter_input
                                        .value
                                        .chars()
                                        .next()
                                        .unwrap_or(',') as u8;
                                let options = ExportOptions {
                                    csv_delimiter: delimiter,
                                    csv_include_header: self.export_modal.csv_include_header,
                                    csv_compression: self.export_modal.csv_compression,
                                    json_compression: self.export_modal.json_compression,
                                    ndjson_compression: self.export_modal.ndjson_compression,
                                    parquet_compression: None,
                                };
                                // Check if file exists and show confirmation
                                if path.exists() {
                                    let path_display = path.display().to_string();
                                    self.pending_export = Some((path, format, options));
                                    self.confirmation_modal.show(format!(
                                        "File already exists:\n{}\n\nDo you wish to overwrite this file?",
                                        path_display
                                    ));
                                    self.export_modal.close();
                                    self.input_mode = InputMode::Normal;
                                } else {
                                    // Start export with progress
                                    self.export_modal.close();
                                    self.input_mode = InputMode::Normal;
                                    return Some(AppEvent::Export(path, format, options));
                                }
                            }
                        }
                        ExportFocus::CancelButton => {
                            self.export_modal.close();
                            self.input_mode = InputMode::Normal;
                        }
                        ExportFocus::CsvIncludeHeader => {
                            self.export_modal.csv_include_header =
                                !self.export_modal.csv_include_header;
                        }
                        ExportFocus::CsvCompression
                        | ExportFocus::JsonCompression
                        | ExportFocus::NdjsonCompression => {
                            // Enter to select current compression option
                            // (Already selected via Left/Right navigation)
                        }
                        _ => {}
                    }
                }
                KeyCode::Char(' ') => {
                    // Space to toggle checkboxes, but pass to text inputs if they're focused
                    match self.export_modal.focus {
                        ExportFocus::PathInput => {
                            // Pass spacebar to text input
                            self.export_modal.path_input.handle_key(event, None);
                        }
                        ExportFocus::CsvDelimiter => {
                            // Pass spacebar to text input
                            self.export_modal
                                .csv_delimiter_input
                                .handle_key(event, None);
                        }
                        ExportFocus::CsvIncludeHeader => {
                            // Toggle checkbox
                            self.export_modal.csv_include_header =
                                !self.export_modal.csv_include_header;
                        }
                        _ => {}
                    }
                }
                KeyCode::Char(_)
                | KeyCode::Backspace
                | KeyCode::Delete
                | KeyCode::Home
                | KeyCode::End => {
                    match self.export_modal.focus {
                        ExportFocus::PathInput => {
                            self.export_modal.path_input.handle_key(event, None);
                        }
                        ExportFocus::CsvDelimiter => {
                            self.export_modal
                                .csv_delimiter_input
                                .handle_key(event, None);
                        }
                        ExportFocus::FormatSelector => {
                            // Don't input text in format selector
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
            return None;
        }

        if self.input_mode == InputMode::PivotMelt {
            let pivot_melt_text_focus = matches!(
                self.pivot_melt_modal.focus,
                PivotMeltFocus::PivotFilter
                    | PivotMeltFocus::MeltFilter
                    | PivotMeltFocus::MeltPattern
                    | PivotMeltFocus::MeltVarName
                    | PivotMeltFocus::MeltValName
            );
            let ctrl_help = event.modifiers.contains(KeyModifiers::CONTROL);
            if event.code == KeyCode::Char('?') && (ctrl_help || !pivot_melt_text_focus) {
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
                        self.pivot_melt_modal
                            .pivot_filter_input
                            .handle_key(event, None);
                        self.pivot_melt_modal.pivot_index_table.select(None);
                    } else if self.pivot_melt_modal.focus == PivotMeltFocus::MeltFilter {
                        self.pivot_melt_modal
                            .melt_filter_input
                            .handle_key(event, None);
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
                KeyCode::Home
                | KeyCode::End
                | KeyCode::Char(_)
                | KeyCode::Backspace
                | KeyCode::Delete
                    if self.pivot_melt_modal.focus == PivotMeltFocus::PivotFilter =>
                {
                    self.pivot_melt_modal
                        .pivot_filter_input
                        .handle_key(event, None);
                    self.pivot_melt_modal.pivot_index_table.select(None);
                }
                KeyCode::Home
                | KeyCode::End
                | KeyCode::Char(_)
                | KeyCode::Backspace
                | KeyCode::Delete
                    if self.pivot_melt_modal.focus == PivotMeltFocus::MeltFilter =>
                {
                    self.pivot_melt_modal
                        .melt_filter_input
                        .handle_key(event, None);
                    self.pivot_melt_modal.melt_index_table.select(None);
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

        if self.input_mode == InputMode::Info {
            let on_tab_bar = self.info_modal.focus == InfoFocus::TabBar;
            let on_body = self.info_modal.focus == InfoFocus::Body;
            let schema_tab = self.info_modal.active_tab == InfoTab::Schema;
            let total_rows = self
                .data_table_state
                .as_ref()
                .map(|s| s.schema.len())
                .unwrap_or(0);
            let visible = self.info_modal.schema_visible_height;

            match event.code {
                KeyCode::Esc | KeyCode::Char('i') if event.is_press() => {
                    self.info_modal.close();
                    self.input_mode = InputMode::Normal;
                }
                KeyCode::Tab if event.is_press() => {
                    if schema_tab {
                        self.info_modal.next_focus();
                    }
                }
                KeyCode::BackTab if event.is_press() => {
                    if schema_tab {
                        self.info_modal.prev_focus();
                    }
                }
                KeyCode::Left | KeyCode::Char('h') if event.is_press() && on_tab_bar => {
                    let has_partitions = self
                        .data_table_state
                        .as_ref()
                        .and_then(|s| s.partition_columns.as_ref())
                        .map(|v| !v.is_empty())
                        .unwrap_or(false);
                    self.info_modal.switch_tab_prev(has_partitions);
                }
                KeyCode::Right | KeyCode::Char('l') if event.is_press() && on_tab_bar => {
                    let has_partitions = self
                        .data_table_state
                        .as_ref()
                        .and_then(|s| s.partition_columns.as_ref())
                        .map(|v| !v.is_empty())
                        .unwrap_or(false);
                    self.info_modal.switch_tab(has_partitions);
                }
                KeyCode::Down | KeyCode::Char('j') if event.is_press() && on_body && schema_tab => {
                    self.info_modal.schema_table_down(total_rows, visible);
                }
                KeyCode::Up | KeyCode::Char('k') if event.is_press() && on_body && schema_tab => {
                    self.info_modal.schema_table_up(total_rows, visible);
                }
                _ => {}
            }
            return None;
        }

        if self.input_mode == InputMode::Chart {
            // Chart export modal (sub-dialog within Chart mode)
            if self.chart_export_modal.active {
                match event.code {
                    KeyCode::Esc if event.is_press() => {
                        self.chart_export_modal.close();
                    }
                    KeyCode::Tab if event.is_press() => {
                        self.chart_export_modal.next_focus();
                    }
                    KeyCode::BackTab if event.is_press() => {
                        self.chart_export_modal.prev_focus();
                    }
                    // Only use h/j/k/l and arrows for format selector; when path input focused, pass all keys to path input
                    KeyCode::Left | KeyCode::Char('h')
                        if event.is_press()
                            && self.chart_export_modal.focus
                                == ChartExportFocus::FormatSelector =>
                    {
                        let idx = ChartExportFormat::ALL
                            .iter()
                            .position(|&f| f == self.chart_export_modal.selected_format)
                            .unwrap_or(0);
                        let prev = if idx == 0 {
                            ChartExportFormat::ALL.len() - 1
                        } else {
                            idx - 1
                        };
                        self.chart_export_modal.selected_format = ChartExportFormat::ALL[prev];
                    }
                    KeyCode::Right | KeyCode::Char('l')
                        if event.is_press()
                            && self.chart_export_modal.focus
                                == ChartExportFocus::FormatSelector =>
                    {
                        let idx = ChartExportFormat::ALL
                            .iter()
                            .position(|&f| f == self.chart_export_modal.selected_format)
                            .unwrap_or(0);
                        let next = (idx + 1) % ChartExportFormat::ALL.len();
                        self.chart_export_modal.selected_format = ChartExportFormat::ALL[next];
                    }
                    KeyCode::Enter if event.is_press() => match self.chart_export_modal.focus {
                        ChartExportFocus::PathInput | ChartExportFocus::ExportButton => {
                            let path_str = self.chart_export_modal.path_input.value.trim();
                            if !path_str.is_empty() {
                                let title =
                                    self.chart_export_modal.title_input.value.trim().to_string();
                                let mut path = PathBuf::from(path_str);
                                let format = self.chart_export_modal.selected_format;
                                // Only add default extension when user did not provide one
                                if path.extension().is_none() {
                                    path.set_extension(format.extension());
                                }
                                let path_display = path.display().to_string();
                                if path.exists() {
                                    self.pending_chart_export = Some((path, format, title));
                                    self.chart_export_modal.close();
                                    self.confirmation_modal.show(format!(
                                            "File already exists:\n{}\n\nDo you wish to overwrite this file?",
                                            path_display
                                        ));
                                } else {
                                    self.chart_export_modal.close();
                                    return Some(AppEvent::ChartExport(path, format, title));
                                }
                            }
                        }
                        ChartExportFocus::CancelButton => {
                            self.chart_export_modal.close();
                        }
                        _ => {}
                    },
                    _ => {
                        if event.is_press() {
                            if self.chart_export_modal.focus == ChartExportFocus::TitleInput {
                                let _ = self.chart_export_modal.title_input.handle_key(event, None);
                            } else if self.chart_export_modal.focus == ChartExportFocus::PathInput {
                                let _ = self.chart_export_modal.path_input.handle_key(event, None);
                            }
                        }
                    }
                }
                return None;
            }

            match event.code {
                KeyCode::Char('e')
                    if event.is_press() && !self.chart_modal.is_text_input_focused() =>
                {
                    // Open chart export modal when there is something visible to export
                    if self.data_table_state.is_some() && self.chart_modal.can_export() {
                        self.chart_export_modal
                            .open(&self.theme, self.history_limit);
                    }
                }
                // q/Q do nothing in chart view (no exit)
                KeyCode::Char('?') if event.is_press() => {
                    self.show_help = true;
                }
                KeyCode::Esc if event.is_press() => {
                    self.chart_modal.close();
                    self.chart_cache.clear();
                    self.input_mode = InputMode::Normal;
                }
                KeyCode::Tab if event.is_press() => {
                    self.chart_modal.next_focus();
                }
                KeyCode::BackTab if event.is_press() => {
                    self.chart_modal.prev_focus();
                }
                KeyCode::Enter | KeyCode::Char(' ') if event.is_press() => {
                    match self.chart_modal.focus {
                        ChartFocus::YStartsAtZero => self.chart_modal.toggle_y_starts_at_zero(),
                        ChartFocus::LogScale => self.chart_modal.toggle_log_scale(),
                        ChartFocus::ShowLegend => self.chart_modal.toggle_show_legend(),
                        ChartFocus::XList => self.chart_modal.x_list_toggle(),
                        ChartFocus::YList => self.chart_modal.y_list_toggle(),
                        ChartFocus::ChartType => self.chart_modal.next_chart_type(),
                        ChartFocus::HistList => self.chart_modal.hist_list_toggle(),
                        ChartFocus::BoxList => self.chart_modal.box_list_toggle(),
                        ChartFocus::KdeList => self.chart_modal.kde_list_toggle(),
                        ChartFocus::HeatmapXList => self.chart_modal.heatmap_x_list_toggle(),
                        ChartFocus::HeatmapYList => self.chart_modal.heatmap_y_list_toggle(),
                        _ => {}
                    }
                }
                KeyCode::Char('+') | KeyCode::Char('=')
                    if event.is_press() && !self.chart_modal.is_text_input_focused() =>
                {
                    match self.chart_modal.focus {
                        ChartFocus::HistBins => self.chart_modal.adjust_hist_bins(1),
                        ChartFocus::HeatmapBins => self.chart_modal.adjust_heatmap_bins(1),
                        ChartFocus::KdeBandwidth => self
                            .chart_modal
                            .adjust_kde_bandwidth_factor(chart_modal::KDE_BANDWIDTH_STEP),
                        ChartFocus::LimitRows => self.chart_modal.adjust_row_limit(1),
                        _ => {}
                    }
                }
                KeyCode::Char('-')
                    if event.is_press() && !self.chart_modal.is_text_input_focused() =>
                {
                    match self.chart_modal.focus {
                        ChartFocus::HistBins => self.chart_modal.adjust_hist_bins(-1),
                        ChartFocus::HeatmapBins => self.chart_modal.adjust_heatmap_bins(-1),
                        ChartFocus::KdeBandwidth => self
                            .chart_modal
                            .adjust_kde_bandwidth_factor(-chart_modal::KDE_BANDWIDTH_STEP),
                        ChartFocus::LimitRows => self.chart_modal.adjust_row_limit(-1),
                        _ => {}
                    }
                }
                KeyCode::Left | KeyCode::Char('h')
                    if event.is_press() && !self.chart_modal.is_text_input_focused() =>
                {
                    match self.chart_modal.focus {
                        ChartFocus::TabBar => self.chart_modal.prev_chart_kind(),
                        ChartFocus::ChartType => self.chart_modal.prev_chart_type(),
                        ChartFocus::HistBins => self.chart_modal.adjust_hist_bins(-1),
                        ChartFocus::HeatmapBins => self.chart_modal.adjust_heatmap_bins(-1),
                        ChartFocus::KdeBandwidth => self
                            .chart_modal
                            .adjust_kde_bandwidth_factor(-chart_modal::KDE_BANDWIDTH_STEP),
                        ChartFocus::LimitRows => self.chart_modal.adjust_row_limit(-1),
                        _ => {}
                    }
                }
                KeyCode::Right | KeyCode::Char('l')
                    if event.is_press() && !self.chart_modal.is_text_input_focused() =>
                {
                    match self.chart_modal.focus {
                        ChartFocus::TabBar => self.chart_modal.next_chart_kind(),
                        ChartFocus::ChartType => self.chart_modal.next_chart_type(),
                        ChartFocus::HistBins => self.chart_modal.adjust_hist_bins(1),
                        ChartFocus::HeatmapBins => self.chart_modal.adjust_heatmap_bins(1),
                        ChartFocus::KdeBandwidth => self
                            .chart_modal
                            .adjust_kde_bandwidth_factor(chart_modal::KDE_BANDWIDTH_STEP),
                        ChartFocus::LimitRows => self.chart_modal.adjust_row_limit(1),
                        _ => {}
                    }
                }
                KeyCode::PageUp
                    if event.is_press() && !self.chart_modal.is_text_input_focused() =>
                {
                    if self.chart_modal.focus == ChartFocus::LimitRows {
                        self.chart_modal.adjust_row_limit_page(1);
                    }
                }
                KeyCode::PageDown
                    if event.is_press() && !self.chart_modal.is_text_input_focused() =>
                {
                    if self.chart_modal.focus == ChartFocus::LimitRows {
                        self.chart_modal.adjust_row_limit_page(-1);
                    }
                }
                KeyCode::Up | KeyCode::Char('k')
                    if event.is_press() && !self.chart_modal.is_text_input_focused() =>
                {
                    match self.chart_modal.focus {
                        ChartFocus::ChartType => self.chart_modal.prev_chart_type(),
                        ChartFocus::XList => self.chart_modal.x_list_up(),
                        ChartFocus::YList => self.chart_modal.y_list_up(),
                        ChartFocus::HistList => self.chart_modal.hist_list_up(),
                        ChartFocus::BoxList => self.chart_modal.box_list_up(),
                        ChartFocus::KdeList => self.chart_modal.kde_list_up(),
                        ChartFocus::HeatmapXList => self.chart_modal.heatmap_x_list_up(),
                        ChartFocus::HeatmapYList => self.chart_modal.heatmap_y_list_up(),
                        _ => {}
                    }
                }
                KeyCode::Down | KeyCode::Char('j')
                    if event.is_press() && !self.chart_modal.is_text_input_focused() =>
                {
                    match self.chart_modal.focus {
                        ChartFocus::ChartType => self.chart_modal.next_chart_type(),
                        ChartFocus::XList => self.chart_modal.x_list_down(),
                        ChartFocus::YList => self.chart_modal.y_list_down(),
                        ChartFocus::HistList => self.chart_modal.hist_list_down(),
                        ChartFocus::BoxList => self.chart_modal.box_list_down(),
                        ChartFocus::KdeList => self.chart_modal.kde_list_down(),
                        ChartFocus::HeatmapXList => self.chart_modal.heatmap_x_list_down(),
                        ChartFocus::HeatmapYList => self.chart_modal.heatmap_y_list_down(),
                        _ => {}
                    }
                }
                _ => {
                    // Pass key to text inputs when focused (including h/j/k/l for typing)
                    if event.is_press() {
                        if self.chart_modal.focus == ChartFocus::XInput {
                            let _ = self.chart_modal.x_input.handle_key(event, None);
                        } else if self.chart_modal.focus == ChartFocus::YInput {
                            let _ = self.chart_modal.y_input.handle_key(event, None);
                        } else if self.chart_modal.focus == ChartFocus::HistInput {
                            let _ = self.chart_modal.hist_input.handle_key(event, None);
                        } else if self.chart_modal.focus == ChartFocus::BoxInput {
                            let _ = self.chart_modal.box_input.handle_key(event, None);
                        } else if self.chart_modal.focus == ChartFocus::KdeInput {
                            let _ = self.chart_modal.kde_input.handle_key(event, None);
                        } else if self.chart_modal.focus == ChartFocus::HeatmapXInput {
                            let _ = self.chart_modal.heatmap_x_input.handle_key(event, None);
                        } else if self.chart_modal.focus == ChartFocus::HeatmapYInput {
                            let _ = self.chart_modal.heatmap_y_input.handle_key(event, None);
                        }
                    }
                }
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
                KeyCode::Char('?') => {
                    self.analysis_modal.show_help = !self.analysis_modal.show_help;
                }
                KeyCode::Char('r') => {
                    if self.sampling_threshold.is_some() {
                        self.analysis_modal.recalculate();
                        self.analysis_modal.analysis_results = None;
                    }
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
                            // Trigger computation for the selected tool if needed
                            match self.analysis_modal.selected_tool {
                                Some(analysis_modal::AnalysisTool::Describe)
                                    if self.analysis_modal.analysis_results.is_none() =>
                                {
                                    self.analysis_modal.computing = Some(AnalysisProgress {
                                        phase: "Describing data".to_string(),
                                        current: 0,
                                        total: 1,
                                    });
                                    self.analysis_computation = Some(AnalysisComputationState {
                                        df: None,
                                        schema: None,
                                        partial_stats: Vec::new(),
                                        current: 0,
                                        total: 0,
                                        total_rows: 0,
                                        sample_seed: self.analysis_modal.random_seed,
                                        sample_size: None,
                                    });
                                    self.busy = true;
                                    return Some(AppEvent::AnalysisChunk);
                                }
                                _ => {
                                    if let Some(ref results) = self.analysis_modal.analysis_results
                                    {
                                        match self.analysis_modal.selected_tool {
                                            Some(
                                                analysis_modal::AnalysisTool::DistributionAnalysis,
                                            ) if results.distribution_analyses.is_empty() => {
                                                self.analysis_modal.computing =
                                                    Some(AnalysisProgress {
                                                        phase: "Distribution".to_string(),
                                                        current: 0,
                                                        total: 1,
                                                    });
                                                self.busy = true;
                                                return Some(AppEvent::AnalysisDistributionCompute);
                                            }
                                            Some(
                                                analysis_modal::AnalysisTool::CorrelationMatrix,
                                            ) if results.correlation_matrix.is_none() => {
                                                self.analysis_modal.computing =
                                                    Some(AnalysisProgress {
                                                        phase: "Correlation".to_string(),
                                                        current: 0,
                                                        total: 1,
                                                    });
                                                self.busy = true;
                                                return Some(AppEvent::AnalysisCorrelationCompute);
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        } else {
                            // Enter in main area opens detail view if applicable
                            match self.analysis_modal.selected_tool {
                                Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                    self.analysis_modal.open_distribution_detail();
                                }
                                Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                    self.analysis_modal.open_correlation_detail();
                                }
                                _ => {}
                            }
                        }
                    }
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
                                        Some(analysis_modal::AnalysisTool::Describe) => {
                                            if let Some(state) = &self.data_table_state {
                                                let max_rows = state.schema.len();
                                                self.analysis_modal.next_row(max_rows);
                                            }
                                        }
                                        Some(
                                            analysis_modal::AnalysisTool::DistributionAnalysis,
                                        ) => {
                                            if let Some(results) =
                                                &self.analysis_modal.analysis_results
                                            {
                                                let max_rows = results.distribution_analyses.len();
                                                self.analysis_modal.next_row(max_rows);
                                            }
                                        }
                                        Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
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
                                        None => {}
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
                KeyCode::Char('s') => {
                    // Toggle histogram scale (linear/log) in distribution detail view
                    if self.analysis_modal.view == analysis_modal::AnalysisView::DistributionDetail
                    {
                        self.analysis_modal.histogram_scale =
                            match self.analysis_modal.histogram_scale {
                                analysis_modal::HistogramScale::Linear => {
                                    analysis_modal::HistogramScale::Log
                                }
                                analysis_modal::HistogramScale::Log => {
                                    analysis_modal::HistogramScale::Linear
                                }
                            };
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
                                    Some(analysis_modal::AnalysisTool::Describe) => {
                                        self.analysis_modal.scroll_left();
                                    }
                                    Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                        self.analysis_modal.scroll_left();
                                    }
                                    Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
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
                                    None => {}
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
                                    Some(analysis_modal::AnalysisTool::Describe) => {
                                        // Number of statistics: count, null_count, mean, std, min, 25%, 50%, 75%, max, skewness, kurtosis, distribution
                                        let max_stats = 12;
                                        // Estimate visible stats based on terminal width (rough estimate)
                                        let visible_stats = 8; // Will be calculated more accurately in widget
                                        self.analysis_modal.scroll_right(max_stats, visible_stats);
                                    }
                                    Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                        // Number of statistics: Distribution, P-value, Shapiro-Wilk, SW p-value, CV, Outliers, Skewness, Kurtosis
                                        let max_stats = 8;
                                        // Estimate visible stats based on terminal width (rough estimate)
                                        let visible_stats = 6; // Will be calculated more accurately in widget
                                        self.analysis_modal.scroll_right(max_stats, visible_stats);
                                    }
                                    Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
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
                                    None => {}
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
                            Some(analysis_modal::AnalysisTool::Describe) => {
                                if let Some(state) = &self.data_table_state {
                                    let max_rows = state.schema.len();
                                    let page_size = 10;
                                    self.analysis_modal.page_down(max_rows, page_size);
                                }
                            }
                            Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                if let Some(results) = &self.analysis_modal.analysis_results {
                                    let max_rows = results.distribution_analyses.len();
                                    let page_size = 10;
                                    self.analysis_modal.page_down(max_rows, page_size);
                                }
                            }
                            Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                if let Some(results) = &self.analysis_modal.analysis_results {
                                    if let Some(corr) = &results.correlation_matrix {
                                        let max_rows = corr.columns.len();
                                        let page_size = 10;
                                        self.analysis_modal.page_down(max_rows, page_size);
                                    }
                                }
                            }
                            None => {}
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
                                    Some(analysis_modal::AnalysisTool::Describe) => {
                                        self.analysis_modal.table_state.select(Some(0));
                                    }
                                    Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                        self.analysis_modal
                                            .distribution_table_state
                                            .select(Some(0));
                                    }
                                    Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                        self.analysis_modal.correlation_table_state.select(Some(0));
                                        self.analysis_modal.selected_correlation = Some((0, 0));
                                    }
                                    None => {}
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
                                    Some(analysis_modal::AnalysisTool::Describe) => {
                                        if let Some(state) = &self.data_table_state {
                                            let max_rows = state.schema.len();
                                            if max_rows > 0 {
                                                self.analysis_modal
                                                    .table_state
                                                    .select(Some(max_rows - 1));
                                            }
                                        }
                                    }
                                    Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
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
                                    Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
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
                                    None => {}
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
                    self.template_modal
                        .enter_create_mode(self.history_limit, &self.theme);
                    // Auto-populate fields
                    if let Some(ref path) = self.path {
                        // Auto-populate name
                        self.template_modal.create_name_input.value =
                            self.template_manager.generate_next_template_name();
                        self.template_modal.create_name_input.cursor =
                            self.template_modal.create_name_input.value.chars().count();

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
                        self.template_modal.create_exact_path_input.value =
                            absolute_path.to_string_lossy().to_string();
                        self.template_modal.create_exact_path_input.cursor = self
                            .template_modal
                            .create_exact_path_input
                            .value
                            .chars()
                            .count();

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
                                    self.template_modal.create_relative_path_input.value =
                                        rel_str.strip_prefix('/').unwrap_or(&rel_str).to_string();
                                    self.template_modal.create_relative_path_input.cursor = self
                                        .template_modal
                                        .create_relative_path_input
                                        .value
                                        .chars()
                                        .count();
                                } else {
                                    // Path is not under CWD, leave empty or use full path
                                    self.template_modal.create_relative_path_input.clear();
                                }
                            } else {
                                // Fallback: try without canonicalization
                                if let Ok(rel_path) = abs_path.strip_prefix(&cwd) {
                                    let rel_str = rel_path.to_string_lossy().to_string();
                                    self.template_modal.create_relative_path_input.value =
                                        rel_str.strip_prefix('/').unwrap_or(&rel_str).to_string();
                                    self.template_modal.create_relative_path_input.cursor = self
                                        .template_modal
                                        .create_relative_path_input
                                        .value
                                        .chars()
                                        .count();
                                } else {
                                    self.template_modal.create_relative_path_input.clear();
                                }
                            }
                        } else {
                            self.template_modal.create_relative_path_input.clear();
                        }

                        // Suggest path pattern
                        if let Some(parent) = path.parent() {
                            if let Some(parent_str) = parent.to_str() {
                                if path.file_name().is_some() {
                                    if let Some(ext) = path.extension() {
                                        self.template_modal.create_path_pattern_input.value =
                                            format!("{}/*.{}", parent_str, ext.to_string_lossy());
                                        self.template_modal.create_path_pattern_input.cursor = self
                                            .template_modal
                                            .create_path_pattern_input
                                            .value
                                            .chars()
                                            .count();
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
                                self.template_modal.create_filename_pattern_input.value = pattern;
                                self.template_modal.create_filename_pattern_input.cursor = self
                                    .template_modal
                                    .create_filename_pattern_input
                                    .value
                                    .chars()
                                    .count();
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
                            self.template_modal.enter_edit_mode(
                                &template_clone,
                                self.history_limit,
                                &self.theme,
                            );
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
                            if self.template_manager.delete_template(&template.id).is_err() {
                                // Delete failed; list will be unchanged
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
                                if self.template_manager.delete_template(&template.id).is_err() {
                                    // Delete failed; list will be unchanged
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
                                let event = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
                                self.template_modal
                                    .create_description_input
                                    .handle_key(&event, None);
                                // Auto-scroll to keep cursor visible
                                let area_height = 10; // Estimate, will be adjusted in rendering
                                self.template_modal
                                    .create_description_input
                                    .ensure_cursor_visible(area_height, 80);
                                return None;
                            }
                            match self.template_modal.create_focus {
                                CreateFocus::SaveButton => {
                                    // Validate name
                                    self.template_modal.name_error = None;
                                    if self
                                        .template_modal
                                        .create_name_input
                                        .value
                                        .trim()
                                        .is_empty()
                                    {
                                        self.template_modal.name_error =
                                            Some("(required)".to_string());
                                        self.template_modal.create_focus = CreateFocus::Name;
                                        return None;
                                    }

                                    // Check for duplicate name (only if creating new, not editing)
                                    if self.template_modal.editing_template_id.is_none()
                                        && self.template_manager.template_exists(
                                            self.template_modal.create_name_input.value.trim(),
                                        )
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
                                            .create_exact_path_input
                                            .value
                                            .trim()
                                            .is_empty()
                                        {
                                            Some(std::path::PathBuf::from(
                                                self.template_modal
                                                    .create_exact_path_input
                                                    .value
                                                    .trim(),
                                            ))
                                        } else {
                                            None
                                        },
                                        relative_path: if !self
                                            .template_modal
                                            .create_relative_path_input
                                            .value
                                            .trim()
                                            .is_empty()
                                        {
                                            Some(
                                                self.template_modal
                                                    .create_relative_path_input
                                                    .value
                                                    .trim()
                                                    .to_string(),
                                            )
                                        } else {
                                            None
                                        },
                                        path_pattern: if !self
                                            .template_modal
                                            .create_path_pattern_input
                                            .value
                                            .is_empty()
                                        {
                                            Some(
                                                self.template_modal
                                                    .create_path_pattern_input
                                                    .value
                                                    .clone(),
                                            )
                                        } else {
                                            None
                                        },
                                        filename_pattern: if !self
                                            .template_modal
                                            .create_filename_pattern_input
                                            .value
                                            .is_empty()
                                        {
                                            Some(
                                                self.template_modal
                                                    .create_filename_pattern_input
                                                    .value
                                                    .clone(),
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

                                    let description = if !self
                                        .template_modal
                                        .create_description_input
                                        .value
                                        .is_empty()
                                    {
                                        Some(
                                            self.template_modal
                                                .create_description_input
                                                .value
                                                .clone(),
                                        )
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
                                            template.name = self
                                                .template_modal
                                                .create_name_input
                                                .value
                                                .trim()
                                                .to_string();
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
                                                    pivot: state.last_pivot_spec().cloned(),
                                                    melt: state.last_melt_spec().cloned(),
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
                                                Err(_) => {
                                                    // Update failed; stay in edit mode
                                                }
                                            }
                                        }
                                    } else {
                                        // Create new template
                                        match self.create_template_from_current_state(
                                            self.template_modal
                                                .create_name_input
                                                .value
                                                .trim()
                                                .to_string(),
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
                                            Err(_) => {
                                                // Create failed; stay in create mode
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
                                let event = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
                                self.template_modal
                                    .create_description_input
                                    .handle_key(&event, None);
                                // Auto-scroll to keep cursor visible
                                let area_height = 10; // Estimate, will be adjusted in rendering
                                self.template_modal
                                    .create_description_input
                                    .ensure_cursor_visible(area_height, 80);
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
                                let event = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
                                self.template_modal
                                    .create_description_input
                                    .handle_key(&event, None);
                                // Auto-scroll to keep cursor visible
                                let area_height = 10; // Estimate, will be adjusted in rendering
                                self.template_modal
                                    .create_description_input
                                    .ensure_cursor_visible(area_height, 80);
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
                            let event = KeyEvent::new(KeyCode::Char(c), KeyModifiers::empty());
                            self.template_modal
                                .create_name_input
                                .handle_key(&event, None);
                        }
                        CreateFocus::Description => {
                            let event = KeyEvent::new(KeyCode::Char(c), KeyModifiers::empty());
                            self.template_modal
                                .create_description_input
                                .handle_key(&event, None);
                            // Auto-scroll to keep cursor visible
                            let area_height = 10; // Estimate, will be adjusted in rendering
                            self.template_modal
                                .create_description_input
                                .ensure_cursor_visible(area_height, 80);
                        }
                        CreateFocus::ExactPath => {
                            let event = KeyEvent::new(KeyCode::Char(c), KeyModifiers::empty());
                            self.template_modal
                                .create_exact_path_input
                                .handle_key(&event, None);
                        }
                        CreateFocus::RelativePath => {
                            let event = KeyEvent::new(KeyCode::Char(c), KeyModifiers::empty());
                            self.template_modal
                                .create_relative_path_input
                                .handle_key(&event, None);
                        }
                        CreateFocus::PathPattern => {
                            let event = KeyEvent::new(KeyCode::Char(c), KeyModifiers::empty());
                            self.template_modal
                                .create_path_pattern_input
                                .handle_key(&event, None);
                        }
                        CreateFocus::FilenamePattern => {
                            let event = KeyEvent::new(KeyCode::Char(c), KeyModifiers::empty());
                            self.template_modal
                                .create_filename_pattern_input
                                .handle_key(&event, None);
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
                KeyCode::Left | KeyCode::Right | KeyCode::Home | KeyCode::End
                    if self.template_modal.mode == TemplateModalMode::Create
                        || self.template_modal.mode == TemplateModalMode::Edit =>
                {
                    match self.template_modal.create_focus {
                        CreateFocus::Name => {
                            self.template_modal
                                .create_name_input
                                .handle_key(event, None);
                        }
                        CreateFocus::Description => {
                            self.template_modal
                                .create_description_input
                                .handle_key(event, None);
                            // Auto-scroll to keep cursor visible
                            let area_height = 10;
                            self.template_modal
                                .create_description_input
                                .ensure_cursor_visible(area_height, 80);
                        }
                        CreateFocus::ExactPath => {
                            self.template_modal
                                .create_exact_path_input
                                .handle_key(event, None);
                        }
                        CreateFocus::RelativePath => {
                            self.template_modal
                                .create_relative_path_input
                                .handle_key(event, None);
                        }
                        CreateFocus::PathPattern => {
                            self.template_modal
                                .create_path_pattern_input
                                .handle_key(event, None);
                        }
                        CreateFocus::FilenamePattern => {
                            self.template_modal
                                .create_filename_pattern_input
                                .handle_key(event, None);
                        }
                        _ => {}
                    }
                }
                KeyCode::PageUp | KeyCode::PageDown
                    if self.template_modal.mode == TemplateModalMode::Create
                        || self.template_modal.mode == TemplateModalMode::Edit =>
                {
                    // PageUp/PageDown for description field - move cursor up/down by 5 lines
                    // This is handled manually since MultiLineTextInput doesn't have built-in PageUp/PageDown
                    if self.template_modal.create_focus == CreateFocus::Description {
                        let lines: Vec<&str> = self
                            .template_modal
                            .create_description_input
                            .value
                            .lines()
                            .collect();
                        let current_line = self.template_modal.create_description_input.cursor_line;
                        let current_col = self.template_modal.create_description_input.cursor_col;

                        let target_line = if event.code == KeyCode::PageUp {
                            current_line.saturating_sub(5)
                        } else {
                            (current_line + 5).min(lines.len().saturating_sub(1))
                        };

                        if target_line < lines.len() {
                            let target_line_str = lines.get(target_line).unwrap_or(&"");
                            let new_col = current_col.min(target_line_str.chars().count());
                            self.template_modal.create_description_input.cursor = self
                                .template_modal
                                .create_description_input
                                .line_col_to_cursor(target_line, new_col);
                            self.template_modal
                                .create_description_input
                                .update_line_col_from_cursor();
                            // Auto-scroll
                            let area_height = 10;
                            self.template_modal
                                .create_description_input
                                .ensure_cursor_visible(area_height, 80);
                        }
                    }
                }
                KeyCode::Backspace
                | KeyCode::Delete
                | KeyCode::Left
                | KeyCode::Right
                | KeyCode::Home
                | KeyCode::End
                    if self.template_modal.mode == TemplateModalMode::Create
                        || self.template_modal.mode == TemplateModalMode::Edit =>
                {
                    match self.template_modal.create_focus {
                        CreateFocus::Name => {
                            self.template_modal
                                .create_name_input
                                .handle_key(event, None);
                        }
                        CreateFocus::Description => {
                            self.template_modal
                                .create_description_input
                                .handle_key(event, None);
                            // Auto-scroll to keep cursor visible
                            let area_height = 10;
                            self.template_modal
                                .create_description_input
                                .ensure_cursor_visible(area_height, 80);
                        }
                        CreateFocus::ExactPath => {
                            self.template_modal
                                .create_exact_path_input
                                .handle_key(event, None);
                        }
                        CreateFocus::RelativePath => {
                            self.template_modal
                                .create_relative_path_input
                                .handle_key(event, None);
                        }
                        CreateFocus::PathPattern => {
                            self.template_modal
                                .create_path_pattern_input
                                .handle_key(event, None);
                        }
                        CreateFocus::FilenamePattern => {
                            self.template_modal
                                .create_filename_pattern_input
                                .handle_key(event, None);
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
            return None;
        }

        if self.input_mode == InputMode::Editing {
            // Use TextInput widget for query input (Search type)
            if self.input_type == Some(InputType::Search) {
                self.query_input.set_focused(true);
                let result = self.query_input.handle_key(event, Some(&self.cache));

                match result {
                    TextInputEvent::Submit => {
                        // Save to history and execute query
                        let _ = self.query_input.save_to_history(&self.cache);
                        let query = self.query_input.value.clone();
                        self.query_input.set_focused(false);
                        return Some(AppEvent::Search(query));
                    }
                    TextInputEvent::Cancel => {
                        // Clear and exit input mode
                        self.query_input.clear();
                        self.query_input.set_focused(false);
                        self.input_mode = InputMode::Normal;
                        if let Some(state) = &mut self.data_table_state {
                            // Clear error and re-enable error display in main view
                            state.error = None;
                            state.suppress_error_display = false;
                        }
                    }
                    TextInputEvent::HistoryChanged => {
                        // History navigation occurred, nothing special needed
                    }
                    TextInputEvent::None => {
                        // Regular input, nothing special needed
                    }
                }
                return None;
            }

            // Line number input (GoToLine): ":" then type line number, Enter to jump, Esc to cancel
            if self.input_type == Some(InputType::GoToLine) {
                self.query_input.set_focused(true);
                let result = self.query_input.handle_key(event, None);
                match result {
                    TextInputEvent::Submit => {
                        let value = self.query_input.value.trim().to_string();
                        self.query_input.clear();
                        self.query_input.set_focused(false);
                        self.input_mode = InputMode::Normal;
                        self.input_type = None;
                        if let Some(state) = &mut self.data_table_state {
                            if let Ok(display_line) = value.parse::<usize>() {
                                let row_index =
                                    display_line.saturating_sub(state.row_start_index());
                                let would_collect = state.scroll_would_trigger_collect(
                                    row_index as i64 - state.start_row as i64,
                                );
                                if would_collect {
                                    self.busy = true;
                                    return Some(AppEvent::GoToLine(row_index));
                                }
                                state.scroll_to_row_centered(row_index);
                            }
                        }
                    }
                    TextInputEvent::Cancel => {
                        self.query_input.clear();
                        self.query_input.set_focused(false);
                        self.input_mode = InputMode::Normal;
                        self.input_type = None;
                    }
                    TextInputEvent::HistoryChanged | TextInputEvent::None => {}
                }
                return None;
            }

            // For other input types (Filter, etc.), keep old behavior for now
            // TODO: Migrate these in later phases
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
                        let _ = state.drill_up();
                        return None;
                    }
                }
                // Escape no longer exits - use 'q' or Ctrl-C to exit
                // (Info modal handles Esc in its own block)
                None
            }
            code if RIGHT_KEYS.contains(&code) => {
                if let Some(ref mut state) = self.data_table_state {
                    state.scroll_right();
                    if self.debug.enabled {
                        self.debug.last_action = "scroll_right".to_string();
                    }
                }
                None
            }
            code if LEFT_KEYS.contains(&code) => {
                if let Some(ref mut state) = self.data_table_state {
                    state.scroll_left();
                    if self.debug.enabled {
                        self.debug.last_action = "scroll_left".to_string();
                    }
                }
                None
            }
            code if event.is_press() && DOWN_KEYS.contains(&code) => {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(1))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollNext)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.select_next();
                    }
                    None
                }
            }
            code if event.is_press() && UP_KEYS.contains(&code) => {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(-1))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollPrev)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.select_previous();
                    }
                    None
                }
            }
            KeyCode::PageDown if event.is_press() => {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(s.visible_rows as i64))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollDown)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.page_down();
                    }
                    None
                }
            }
            KeyCode::Home if event.is_press() => {
                if let Some(ref mut state) = self.data_table_state {
                    if state.start_row > 0 {
                        state.scroll_to(0);
                    }
                    state.table_state.select(Some(0));
                }
                None
            }
            KeyCode::End if event.is_press() => {
                if self.data_table_state.is_some() {
                    self.busy = true;
                    Some(AppEvent::DoScrollEnd)
                } else {
                    None
                }
            }
            KeyCode::Char('G') if event.is_press() => {
                if self.data_table_state.is_some() {
                    self.busy = true;
                    Some(AppEvent::DoScrollEnd)
                } else {
                    None
                }
            }
            KeyCode::Char('f')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(s.visible_rows as i64))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollDown)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.page_down();
                    }
                    None
                }
            }
            KeyCode::Char('b')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(-(s.visible_rows as i64)))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollUp)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.page_up();
                    }
                    None
                }
            }
            KeyCode::Char('d')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                let half = self
                    .data_table_state
                    .as_ref()
                    .map(|s| (s.visible_rows / 2).max(1) as i64)
                    .unwrap_or(1);
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(half))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollHalfDown)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.half_page_down();
                    }
                    None
                }
            }
            KeyCode::Char('u')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                let half = self
                    .data_table_state
                    .as_ref()
                    .map(|s| (s.visible_rows / 2).max(1) as i64)
                    .unwrap_or(1);
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(-half))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollHalfUp)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.half_page_up();
                    }
                    None
                }
            }
            KeyCode::PageUp if event.is_press() => {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(-(s.visible_rows as i64)))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollUp)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.page_up();
                    }
                    None
                }
            }
            KeyCode::Enter if event.is_press() => {
                // Only drill down if not in a modal and viewing grouped data
                if self.input_mode == InputMode::Normal {
                    if let Some(ref mut state) = self.data_table_state {
                        if state.is_grouped() && !state.is_drilled_down() {
                            if let Some(selected) = state.table_state.selected() {
                                let group_index = state.start_row + selected;
                                let _ = state.drill_down_into_group(group_index);
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
            KeyCode::BackTab if event.is_press() => {
                self.focus = (self.focus + 1) % 2;
                None
            }
            KeyCode::Char('i') if event.is_press() => {
                if self.data_table_state.is_some() {
                    self.info_modal.open();
                    self.input_mode = InputMode::Info;
                    // Defer Parquet metadata load so UI can show throbber; avoid blocking in render
                    if self.path.is_some()
                        && self.original_file_format == Some(ExportFormat::Parquet)
                        && self.parquet_metadata_cache.is_none()
                    {
                        self.busy = true;
                        return Some(AppEvent::DoLoadParquetMetadata);
                    }
                }
                None
            }
            KeyCode::Char('/') => {
                self.input_mode = InputMode::Editing;
                self.input_type = Some(InputType::Search);
                // Initialize query input with current query if available
                if let Some(state) = &mut self.data_table_state {
                    self.query_input.value = state.active_query.clone();
                    self.query_input.cursor = self.query_input.value.chars().count();
                    // Suppress error display in main view when query input is active
                    state.suppress_error_display = true;
                } else {
                    self.query_input.clear();
                }
                self.query_input.set_focused(true);
                None
            }
            KeyCode::Char(':') if event.is_press() => {
                if self.data_table_state.is_some() {
                    self.input_mode = InputMode::Editing;
                    self.input_type = Some(InputType::GoToLine);
                    self.query_input.value.clear();
                    self.query_input.cursor = 0;
                    self.query_input.set_focused(true);
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
                    let headers: Vec<String> =
                        state.schema.iter_names().map(|s| s.to_string()).collect();
                    let locked_count = state.locked_columns_count();

                    // Populate sort tab
                    let mut existing_columns: std::collections::HashMap<String, SortColumn> = self
                        .sort_filter_modal
                        .sort
                        .columns
                        .iter()
                        .map(|c| (c.name.clone(), c.clone()))
                        .collect();
                    self.sort_filter_modal.sort.columns = headers
                        .iter()
                        .enumerate()
                        .map(|(i, h)| {
                            if let Some(mut col) = existing_columns.remove(h) {
                                col.display_order = i;
                                col.is_locked = i < locked_count;
                                col.is_to_be_locked = false;
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
                    self.sort_filter_modal.sort.filter_input.clear();
                    self.sort_filter_modal.sort.focus = SortFocus::ColumnList;

                    // Populate filter tab
                    self.sort_filter_modal.filter.available_columns = state.headers();
                    if !self.sort_filter_modal.filter.available_columns.is_empty() {
                        self.sort_filter_modal.filter.new_column_idx =
                            self.sort_filter_modal.filter.new_column_idx.min(
                                self.sort_filter_modal
                                    .filter
                                    .available_columns
                                    .len()
                                    .saturating_sub(1),
                            );
                    } else {
                        self.sort_filter_modal.filter.new_column_idx = 0;
                    }

                    self.sort_filter_modal.open(self.history_limit, &self.theme);
                    self.input_mode = InputMode::SortFilter;
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
                // Open analysis modal; no computation until user selects a tool from the sidebar (Enter)
                if self.data_table_state.is_some() && self.input_mode == InputMode::Normal {
                    self.analysis_modal.open();
                    self.analysis_modal.analysis_results = None;
                }
                None
            }
            KeyCode::Char('c') => {
                if let Some(state) = &self.data_table_state {
                    if self.input_mode == InputMode::Normal {
                        let numeric_columns: Vec<String> = state
                            .schema
                            .iter()
                            .filter(|(_, dtype)| {
                                matches!(
                                    dtype,
                                    DataType::Int8
                                        | DataType::Int16
                                        | DataType::Int32
                                        | DataType::Int64
                                        | DataType::UInt8
                                        | DataType::UInt16
                                        | DataType::UInt32
                                        | DataType::UInt64
                                        | DataType::Float32
                                        | DataType::Float64
                                )
                            })
                            .map(|(name, _)| name.to_string())
                            .collect();
                        let datetime_columns: Vec<String> = state
                            .schema
                            .iter()
                            .filter(|(_, dtype)| {
                                matches!(
                                    dtype,
                                    DataType::Datetime(_, _) | DataType::Date | DataType::Time
                                )
                            })
                            .map(|(name, _)| name.to_string())
                            .collect();
                        self.chart_modal.open(
                            &numeric_columns,
                            &datetime_columns,
                            self.app_config.chart.row_limit,
                        );
                        self.chart_modal.x_input =
                            std::mem::take(&mut self.chart_modal.x_input).with_theme(&self.theme);
                        self.chart_modal.y_input =
                            std::mem::take(&mut self.chart_modal.y_input).with_theme(&self.theme);
                        self.chart_modal.hist_input =
                            std::mem::take(&mut self.chart_modal.hist_input)
                                .with_theme(&self.theme);
                        self.chart_modal.box_input =
                            std::mem::take(&mut self.chart_modal.box_input).with_theme(&self.theme);
                        self.chart_modal.kde_input =
                            std::mem::take(&mut self.chart_modal.kde_input).with_theme(&self.theme);
                        self.chart_modal.heatmap_x_input =
                            std::mem::take(&mut self.chart_modal.heatmap_x_input)
                                .with_theme(&self.theme);
                        self.chart_modal.heatmap_y_input =
                            std::mem::take(&mut self.chart_modal.heatmap_y_input)
                                .with_theme(&self.theme);
                        self.chart_cache.clear();
                        self.input_mode = InputMode::Chart;
                    }
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
                        self.pivot_melt_modal.open(self.history_limit, &self.theme);
                        self.input_mode = InputMode::PivotMelt;
                    }
                }
                None
            }
            KeyCode::Char('e') => {
                if self.data_table_state.is_some() && self.input_mode == InputMode::Normal {
                    // Load config to get delimiter preference
                    let config_delimiter = AppConfig::load(APP_NAME)
                        .ok()
                        .and_then(|config| config.file_loading.delimiter);
                    self.export_modal.open(
                        self.original_file_format,
                        self.history_limit,
                        &self.theme,
                        self.original_file_delimiter,
                        config_delimiter,
                    );
                    self.input_mode = InputMode::Export;
                }
                None
            }
            _ => None,
        }
    }

    pub fn event(&mut self, event: &AppEvent) -> Option<AppEvent> {
        self.debug.num_events += 1;
        match event {
            AppEvent::Key(key) => {
                let is_column_scroll = matches!(
                    key.code,
                    KeyCode::Left | KeyCode::Right | KeyCode::Char('h') | KeyCode::Char('l')
                );
                let is_help_key = key.code == KeyCode::F(1);
                // When busy (e.g. loading), still process column scroll and F1 so the user can navigate or open help.
                if self.busy && !is_column_scroll && !is_help_key {
                    return None;
                }
                self.key(key)
            }
            AppEvent::Open(paths, options) => {
                #[cfg(feature = "http")]
                if let Some(ref p) = self.http_temp_path.take() {
                    let _ = std::fs::remove_file(p);
                }
                self.busy = true;
                let first = &paths[0];
                let file_size = match source::input_source(first) {
                    source::InputSource::Local(_) => {
                        std::fs::metadata(first).map(|m| m.len()).unwrap_or(0)
                    }
                    source::InputSource::S3(_)
                    | source::InputSource::Gcs(_)
                    | source::InputSource::Http(_) => 0,
                };
                let path_str = first.as_os_str().to_string_lossy();
                let _is_partitioned_path = paths.len() == 1
                    && options.hive
                    && (first.is_dir() || path_str.contains('*') || path_str.contains("**"));
                let phase = "Scanning input";

                self.loading_state = LoadingState::Loading {
                    file_path: Some(first.clone()),
                    file_size,
                    current_phase: phase.to_string(),
                    progress_percent: 10,
                };

                Some(AppEvent::DoLoadScanPaths(paths.clone(), options.clone()))
            }
            AppEvent::OpenLazyFrame(lf, options) => {
                self.busy = true;
                self.loading_state = LoadingState::Loading {
                    file_path: None,
                    file_size: 0,
                    current_phase: "Scanning input".to_string(),
                    progress_percent: 10,
                };
                Some(AppEvent::DoLoadSchema(lf.clone(), None, options.clone()))
            }
            AppEvent::DoLoadScanPaths(paths, options) => {
                let first = &paths[0];
                let src = source::input_source(first);
                if paths.len() > 1 {
                    match &src {
                        source::InputSource::S3(_) => {
                            return Some(AppEvent::Crash(
                                "Only one S3 URL at a time. Open a single s3:// path.".to_string(),
                            ));
                        }
                        source::InputSource::Gcs(_) => {
                            return Some(AppEvent::Crash(
                                "Only one GCS URL at a time. Open a single gs:// path.".to_string(),
                            ));
                        }
                        source::InputSource::Http(_) => {
                            return Some(AppEvent::Crash(
                                "Only one HTTP/HTTPS URL at a time. Open a single URL.".to_string(),
                            ));
                        }
                        source::InputSource::Local(_) => {}
                    }
                }
                let compression = options
                    .compression
                    .or_else(|| CompressionFormat::from_extension(first));
                let is_csv = first
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .map(|stem| {
                        stem.ends_with(".csv")
                            || first
                                .extension()
                                .and_then(|e| e.to_str())
                                .map(|e| e.eq_ignore_ascii_case("csv"))
                                .unwrap_or(false)
                    })
                    .unwrap_or(false);
                let is_compressed_csv = matches!(src, source::InputSource::Local(_))
                    && paths.len() == 1
                    && compression.is_some()
                    && is_csv;
                if is_compressed_csv {
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
                    Some(AppEvent::DoLoad(paths.clone(), options.clone()))
                } else {
                    #[cfg(feature = "http")]
                    if let source::InputSource::Http(ref url) = src {
                        if let LoadingState::Loading {
                            file_path,
                            file_size,
                            ..
                        } = &self.loading_state
                        {
                            self.loading_state = LoadingState::Loading {
                                file_path: file_path.clone(),
                                file_size: *file_size,
                                current_phase: "Downloading".to_string(),
                                progress_percent: 20,
                            };
                        }
                        return Some(AppEvent::DoDownloadHttp(url.clone(), options.clone()));
                    }
                    #[cfg(feature = "cloud")]
                    if let source::InputSource::S3(ref url) = src {
                        let full = format!("s3://{url}");
                        let (_, ext) = source::url_path_extension(&full);
                        let is_parquet = ext
                            .as_ref()
                            .map(|e| e.eq_ignore_ascii_case("parquet"))
                            .unwrap_or(false);
                        let is_glob = full.contains('*') || full.ends_with('/');
                        if !is_parquet && !is_glob {
                            if let LoadingState::Loading {
                                file_path,
                                file_size,
                                ..
                            } = &self.loading_state
                            {
                                self.loading_state = LoadingState::Loading {
                                    file_path: file_path.clone(),
                                    file_size: *file_size,
                                    current_phase: "Downloading".to_string(),
                                    progress_percent: 20,
                                };
                            }
                            return Some(AppEvent::DoDownloadS3ToTemp(full, options.clone()));
                        }
                    }
                    #[cfg(feature = "cloud")]
                    if let source::InputSource::Gcs(ref url) = src {
                        let full = format!("gs://{url}");
                        let (_, ext) = source::url_path_extension(&full);
                        let is_parquet = ext
                            .as_ref()
                            .map(|e| e.eq_ignore_ascii_case("parquet"))
                            .unwrap_or(false);
                        let is_glob = full.contains('*') || full.ends_with('/');
                        if !is_parquet && !is_glob {
                            if let LoadingState::Loading {
                                file_path,
                                file_size,
                                ..
                            } = &self.loading_state
                            {
                                self.loading_state = LoadingState::Loading {
                                    file_path: file_path.clone(),
                                    file_size: *file_size,
                                    current_phase: "Downloading".to_string(),
                                    progress_percent: 20,
                                };
                            }
                            return Some(AppEvent::DoDownloadGcsToTemp(full, options.clone()));
                        }
                    }
                    let first = paths[0].clone();
                    #[allow(clippy::needless_borrow)]
                    match self.build_lazyframe_from_paths(&paths, options) {
                        Ok(lf) => {
                            if let LoadingState::Loading {
                                file_path,
                                file_size,
                                ..
                            } = &self.loading_state
                            {
                                self.loading_state = LoadingState::Loading {
                                    file_path: file_path.clone(),
                                    file_size: *file_size,
                                    current_phase: "Caching schema".to_string(),
                                    progress_percent: 40,
                                };
                            }
                            Some(AppEvent::DoLoadSchema(
                                Box::new(lf),
                                Some(first),
                                options.clone(),
                            ))
                        }
                        Err(e) => {
                            self.loading_state = LoadingState::Idle;
                            self.busy = false;
                            self.drain_keys_on_next_loop = true;
                            let msg = crate::error_display::user_message_from_report(
                                &e,
                                paths.first().map(|p| p.as_path()),
                            );
                            Some(AppEvent::Crash(msg))
                        }
                    }
                }
            }
            #[cfg(feature = "http")]
            AppEvent::DoDownloadHttp(url, options) => {
                let (_, ext) = source::url_path_extension(url.as_str());
                match Self::download_http_to_temp(
                    url.as_str(),
                    options.temp_dir.as_deref(),
                    ext.as_deref(),
                ) {
                    Ok(temp_path) => {
                        self.http_temp_path = Some(temp_path.clone());
                        if let LoadingState::Loading {
                            file_path,
                            file_size,
                            ..
                        } = &self.loading_state
                        {
                            self.loading_state = LoadingState::Loading {
                                file_path: file_path.clone(),
                                file_size: *file_size,
                                current_phase: "Scanning".to_string(),
                                progress_percent: 30,
                            };
                        }
                        Some(AppEvent::DoLoadFromHttpTemp(temp_path, options.clone()))
                    }
                    Err(e) => {
                        self.loading_state = LoadingState::Idle;
                        self.busy = false;
                        self.drain_keys_on_next_loop = true;
                        let msg = crate::error_display::user_message_from_report(&e, None);
                        Some(AppEvent::Crash(msg))
                    }
                }
            }
            #[cfg(feature = "cloud")]
            AppEvent::DoDownloadS3ToTemp(s3_url, options) => {
                match Self::download_s3_to_temp(s3_url, &self.app_config.cloud, options) {
                    Ok(temp_path) => {
                        self.http_temp_path = Some(temp_path.clone());
                        if let LoadingState::Loading {
                            file_path,
                            file_size,
                            ..
                        } = &self.loading_state
                        {
                            self.loading_state = LoadingState::Loading {
                                file_path: file_path.clone(),
                                file_size: *file_size,
                                current_phase: "Scanning".to_string(),
                                progress_percent: 30,
                            };
                        }
                        Some(AppEvent::DoLoadFromHttpTemp(temp_path, options.clone()))
                    }
                    Err(e) => {
                        self.loading_state = LoadingState::Idle;
                        self.busy = false;
                        self.drain_keys_on_next_loop = true;
                        let msg = crate::error_display::user_message_from_report(&e, None);
                        Some(AppEvent::Crash(msg))
                    }
                }
            }
            #[cfg(feature = "cloud")]
            AppEvent::DoDownloadGcsToTemp(gs_url, options) => {
                match Self::download_gcs_to_temp(gs_url, options) {
                    Ok(temp_path) => {
                        self.http_temp_path = Some(temp_path.clone());
                        if let LoadingState::Loading {
                            file_path,
                            file_size,
                            ..
                        } = &self.loading_state
                        {
                            self.loading_state = LoadingState::Loading {
                                file_path: file_path.clone(),
                                file_size: *file_size,
                                current_phase: "Scanning".to_string(),
                                progress_percent: 30,
                            };
                        }
                        Some(AppEvent::DoLoadFromHttpTemp(temp_path, options.clone()))
                    }
                    Err(e) => {
                        self.loading_state = LoadingState::Idle;
                        self.busy = false;
                        self.drain_keys_on_next_loop = true;
                        let msg = crate::error_display::user_message_from_report(&e, None);
                        Some(AppEvent::Crash(msg))
                    }
                }
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            AppEvent::DoLoadFromHttpTemp(temp_path, options) => {
                self.http_temp_path = Some(temp_path.clone());
                let display_path = match &self.loading_state {
                    LoadingState::Loading { file_path, .. } => file_path.clone(),
                    _ => None,
                };
                if let LoadingState::Loading {
                    file_path,
                    file_size,
                    ..
                } = &self.loading_state
                {
                    self.loading_state = LoadingState::Loading {
                        file_path: file_path.clone(),
                        file_size: *file_size,
                        current_phase: "Scanning".to_string(),
                        progress_percent: 30,
                    };
                }
                #[allow(clippy::cloned_ref_to_slice_refs)]
                match self.build_lazyframe_from_paths(&[temp_path.clone()], options) {
                    Ok(lf) => {
                        if let LoadingState::Loading {
                            file_path,
                            file_size,
                            ..
                        } = &self.loading_state
                        {
                            self.loading_state = LoadingState::Loading {
                                file_path: file_path.clone(),
                                file_size: *file_size,
                                current_phase: "Caching schema".to_string(),
                                progress_percent: 40,
                            };
                        }
                        Some(AppEvent::DoLoadSchema(
                            Box::new(lf),
                            display_path,
                            options.clone(),
                        ))
                    }
                    Err(e) => {
                        self.loading_state = LoadingState::Idle;
                        self.busy = false;
                        self.drain_keys_on_next_loop = true;
                        let msg = crate::error_display::user_message_from_report(
                            &e,
                            Some(temp_path.as_path()),
                        );
                        Some(AppEvent::Crash(msg))
                    }
                }
            }
            AppEvent::DoLoadSchema(lf, path, options) => {
                // Set "Caching schema" and return so the UI draws this phase before we block in DoLoadSchemaBlocking
                if let LoadingState::Loading {
                    file_path,
                    file_size,
                    ..
                } = &self.loading_state
                {
                    self.loading_state = LoadingState::Loading {
                        file_path: file_path.clone(),
                        file_size: *file_size,
                        current_phase: "Caching schema".to_string(),
                        progress_percent: 40,
                    };
                }
                Some(AppEvent::DoLoadSchemaBlocking(
                    lf.clone(),
                    path.clone(),
                    options.clone(),
                ))
            }
            AppEvent::DoLoadSchemaBlocking(lf, path, options) => {
                // Fast path for hive directory: infer schema from one parquet file instead of collect_schema() over all files.
                if path.as_ref().is_some_and(|p| p.is_dir() && options.hive) {
                    let p = path.as_ref().unwrap();
                    if let Ok((merged_schema, partition_columns)) =
                        DataTableState::schema_from_one_hive_parquet(p)
                    {
                        if let Ok(lf_owned) =
                            DataTableState::scan_parquet_hive_with_schema(p, merged_schema.clone())
                        {
                            match DataTableState::from_schema_and_lazyframe(
                                merged_schema,
                                lf_owned,
                                options,
                                Some(partition_columns),
                            ) {
                                Ok(state) => {
                                    self.parquet_metadata_cache = None;
                                    self.export_df = None;
                                    self.data_table_state = Some(state);
                                    self.path = path.clone();
                                    if let Some(ref path_p) = path {
                                        self.original_file_format = path_p
                                            .extension()
                                            .and_then(|e| e.to_str())
                                            .and_then(|ext| {
                                                if ext.eq_ignore_ascii_case("parquet") {
                                                    Some(ExportFormat::Parquet)
                                                } else if ext.eq_ignore_ascii_case("csv") {
                                                    Some(ExportFormat::Csv)
                                                } else if ext.eq_ignore_ascii_case("json") {
                                                    Some(ExportFormat::Json)
                                                } else if ext.eq_ignore_ascii_case("jsonl")
                                                    || ext.eq_ignore_ascii_case("ndjson")
                                                {
                                                    Some(ExportFormat::Ndjson)
                                                } else if ext.eq_ignore_ascii_case("arrow")
                                                    || ext.eq_ignore_ascii_case("ipc")
                                                    || ext.eq_ignore_ascii_case("feather")
                                                {
                                                    Some(ExportFormat::Ipc)
                                                } else if ext.eq_ignore_ascii_case("avro") {
                                                    Some(ExportFormat::Avro)
                                                } else {
                                                    None
                                                }
                                            });
                                        self.original_file_delimiter =
                                            Some(options.delimiter.unwrap_or(b','));
                                    } else {
                                        self.original_file_format = None;
                                        self.original_file_delimiter = None;
                                    }
                                    self.sort_filter_modal = SortFilterModal::new();
                                    self.pivot_melt_modal = PivotMeltModal::new();
                                    if let LoadingState::Loading {
                                        file_path,
                                        file_size,
                                        ..
                                    } = &self.loading_state
                                    {
                                        self.loading_state = LoadingState::Loading {
                                            file_path: file_path.clone(),
                                            file_size: *file_size,
                                            current_phase: "Loading buffer".to_string(),
                                            progress_percent: 70,
                                        };
                                    }
                                    return Some(AppEvent::DoLoadBuffer);
                                }
                                Err(e) => {
                                    self.loading_state = LoadingState::Idle;
                                    self.busy = false;
                                    self.drain_keys_on_next_loop = true;
                                    let msg =
                                        crate::error_display::user_message_from_report(&e, None);
                                    return Some(AppEvent::Crash(msg));
                                }
                            }
                        }
                    }
                }

                let mut lf_owned = (**lf).clone();
                match lf_owned.collect_schema() {
                    Ok(schema) => {
                        let partition_columns = if path.as_ref().is_some_and(|p| {
                            options.hive
                                && (p.is_dir() || p.as_os_str().to_string_lossy().contains('*'))
                        }) {
                            let discovered = DataTableState::discover_hive_partition_columns(
                                path.as_ref().unwrap(),
                            );
                            discovered
                                .into_iter()
                                .filter(|c| schema.contains(c.as_str()))
                                .collect::<Vec<_>>()
                        } else {
                            Vec::new()
                        };
                        if !partition_columns.is_empty() {
                            let exprs: Vec<_> = partition_columns
                                .iter()
                                .map(|s| col(s.as_str()))
                                .chain(
                                    schema
                                        .iter_names()
                                        .map(|s| s.to_string())
                                        .filter(|c| !partition_columns.contains(c))
                                        .map(|s| col(s.as_str())),
                                )
                                .collect();
                            lf_owned = lf_owned.select(exprs);
                        }
                        let part_cols_opt = if partition_columns.is_empty() {
                            None
                        } else {
                            Some(partition_columns)
                        };
                        match DataTableState::from_schema_and_lazyframe(
                            schema,
                            lf_owned,
                            options,
                            part_cols_opt,
                        ) {
                            Ok(state) => {
                                self.parquet_metadata_cache = None;
                                self.export_df = None;
                                self.data_table_state = Some(state);
                                self.path = path.clone();
                                if let Some(ref p) = path {
                                    self.original_file_format =
                                        p.extension().and_then(|e| e.to_str()).and_then(|ext| {
                                            if ext.eq_ignore_ascii_case("parquet") {
                                                Some(ExportFormat::Parquet)
                                            } else if ext.eq_ignore_ascii_case("csv") {
                                                Some(ExportFormat::Csv)
                                            } else if ext.eq_ignore_ascii_case("json") {
                                                Some(ExportFormat::Json)
                                            } else if ext.eq_ignore_ascii_case("jsonl")
                                                || ext.eq_ignore_ascii_case("ndjson")
                                            {
                                                Some(ExportFormat::Ndjson)
                                            } else if ext.eq_ignore_ascii_case("arrow")
                                                || ext.eq_ignore_ascii_case("ipc")
                                                || ext.eq_ignore_ascii_case("feather")
                                            {
                                                Some(ExportFormat::Ipc)
                                            } else if ext.eq_ignore_ascii_case("avro") {
                                                Some(ExportFormat::Avro)
                                            } else {
                                                None
                                            }
                                        });
                                    self.original_file_delimiter =
                                        Some(options.delimiter.unwrap_or(b','));
                                } else {
                                    self.original_file_format = None;
                                    self.original_file_delimiter = None;
                                }
                                self.sort_filter_modal = SortFilterModal::new();
                                self.pivot_melt_modal = PivotMeltModal::new();
                                if let LoadingState::Loading {
                                    file_path,
                                    file_size,
                                    ..
                                } = &self.loading_state
                                {
                                    self.loading_state = LoadingState::Loading {
                                        file_path: file_path.clone(),
                                        file_size: *file_size,
                                        current_phase: "Loading buffer".to_string(),
                                        progress_percent: 70,
                                    };
                                }
                                Some(AppEvent::DoLoadBuffer)
                            }
                            Err(e) => {
                                self.loading_state = LoadingState::Idle;
                                self.busy = false;
                                self.drain_keys_on_next_loop = true;
                                let msg = crate::error_display::user_message_from_report(&e, None);
                                Some(AppEvent::Crash(msg))
                            }
                        }
                    }
                    Err(e) => {
                        self.loading_state = LoadingState::Idle;
                        self.busy = false;
                        self.drain_keys_on_next_loop = true;
                        let report = color_eyre::eyre::Report::from(e);
                        let msg = crate::error_display::user_message_from_report(&report, None);
                        Some(AppEvent::Crash(msg))
                    }
                }
            }
            AppEvent::DoLoadBuffer => {
                if let Some(state) = &mut self.data_table_state {
                    state.collect();
                    if let Some(e) = state.error.take() {
                        self.loading_state = LoadingState::Idle;
                        self.busy = false;
                        self.drain_keys_on_next_loop = true;
                        let msg = crate::error_display::user_message_from_polars(&e);
                        return Some(AppEvent::Crash(msg));
                    }
                }
                self.loading_state = LoadingState::Idle;
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                Some(AppEvent::Collect)
            }
            AppEvent::DoLoad(paths, options) => {
                let first = &paths[0];
                // Check if file is compressed (only single-file compressed CSV supported for now)
                let compression = options
                    .compression
                    .or_else(|| CompressionFormat::from_extension(first));
                let is_csv = first
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .map(|stem| {
                        stem.ends_with(".csv")
                            || first
                                .extension()
                                .and_then(|e| e.to_str())
                                .map(|e| e.eq_ignore_ascii_case("csv"))
                                .unwrap_or(false)
                    })
                    .unwrap_or(false);
                let is_compressed_csv = paths.len() == 1 && compression.is_some() && is_csv;

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
                    Some(AppEvent::DoDecompress(paths.clone(), options.clone()))
                } else {
                    // For non-compressed files, proceed with normal loading
                    match self.load(paths, options) {
                        Ok(_) => {
                            self.busy = false;
                            self.drain_keys_on_next_loop = true;
                            Some(AppEvent::Collect)
                        }
                        Err(e) => {
                            self.loading_state = LoadingState::Idle;
                            self.busy = false;
                            self.drain_keys_on_next_loop = true;
                            let msg = crate::error_display::user_message_from_report(
                                &e,
                                paths.first().map(|p| p.as_path()),
                            );
                            Some(AppEvent::Crash(msg))
                        }
                    }
                }
            }
            AppEvent::DoDecompress(paths, options) => {
                // Actually perform decompression now (after UI has rendered "Decompressing")
                match self.load(paths, options) {
                    Ok(_) => Some(AppEvent::DoLoadBuffer),
                    Err(e) => {
                        self.loading_state = LoadingState::Idle;
                        self.busy = false;
                        self.drain_keys_on_next_loop = true;
                        let msg = crate::error_display::user_message_from_report(
                            &e,
                            paths.first().map(|p| p.as_path()),
                        );
                        Some(AppEvent::Crash(msg))
                    }
                }
            }
            AppEvent::Resize(_cols, rows) => {
                self.busy = true;
                if let Some(state) = &mut self.data_table_state {
                    state.visible_rows = *rows as usize;
                    state.collect();
                }
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                None
            }
            AppEvent::Collect => {
                self.busy = true;
                if let Some(ref mut state) = self.data_table_state {
                    state.collect();
                }
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                None
            }
            AppEvent::DoScrollDown => {
                if let Some(state) = &mut self.data_table_state {
                    state.page_down();
                }
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                None
            }
            AppEvent::DoScrollUp => {
                if let Some(state) = &mut self.data_table_state {
                    state.page_up();
                }
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                None
            }
            AppEvent::DoScrollNext => {
                if let Some(state) = &mut self.data_table_state {
                    state.select_next();
                }
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                None
            }
            AppEvent::DoScrollPrev => {
                if let Some(state) = &mut self.data_table_state {
                    state.select_previous();
                }
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                None
            }
            AppEvent::DoScrollEnd => {
                if let Some(state) = &mut self.data_table_state {
                    state.scroll_to_end();
                }
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                None
            }
            AppEvent::DoScrollHalfDown => {
                if let Some(state) = &mut self.data_table_state {
                    state.half_page_down();
                }
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                None
            }
            AppEvent::DoScrollHalfUp => {
                if let Some(state) = &mut self.data_table_state {
                    state.half_page_up();
                }
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                None
            }
            AppEvent::GoToLine(n) => {
                if let Some(state) = &mut self.data_table_state {
                    state.scroll_to_row_centered(*n);
                }
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                None
            }
            AppEvent::AnalysisChunk => {
                let lf = match &self.data_table_state {
                    Some(state) => state.lf.clone(),
                    None => {
                        self.analysis_computation = None;
                        self.analysis_modal.computing = None;
                        self.busy = false;
                        return None;
                    }
                };
                let comp = self.analysis_computation.take()?;
                if comp.df.is_none() {
                    // First chunk: get row count then run describe (lazy aggregation, no full collect)
                    // Reuse cached row count from control bar when valid to avoid extra full scan.
                    let total_rows = match self
                        .data_table_state
                        .as_ref()
                        .and_then(|s| s.num_rows_if_valid())
                    {
                        Some(n) => n,
                        None => match lf.clone().select([len()]).collect() {
                            Ok(count_df) => {
                                if let Some(col) = count_df.get(0) {
                                    match col.first() {
                                        Some(AnyValue::UInt32(n)) => *n as usize,
                                        _ => 0,
                                    }
                                } else {
                                    0
                                }
                            }
                            Err(_e) => {
                                self.analysis_modal.computing = None;
                                self.busy = false;
                                self.drain_keys_on_next_loop = true;
                                return None;
                            }
                        },
                    };
                    match crate::statistics::compute_describe_from_lazy(
                        &lf,
                        total_rows,
                        self.sampling_threshold,
                        comp.sample_seed,
                    ) {
                        Ok(results) => {
                            self.analysis_modal.analysis_results = Some(results);
                            self.analysis_modal.computing = None;
                            self.busy = false;
                            self.drain_keys_on_next_loop = true;
                            None
                        }
                        Err(_e) => {
                            self.analysis_modal.computing = None;
                            self.busy = false;
                            self.drain_keys_on_next_loop = true;
                            None
                        }
                    }
                } else {
                    None
                }
            }
            AppEvent::AnalysisDistributionCompute => {
                if let Some(state) = &self.data_table_state {
                    if let Some(ref mut results) = self.analysis_modal.analysis_results {
                        if crate::statistics::compute_distribution_statistics(
                            results,
                            &state.lf,
                            self.sampling_threshold,
                            self.analysis_modal.random_seed,
                        )
                        .is_err()
                        {
                            // Distribution computation failed; analysis view will show error state
                        }
                    }
                }
                self.analysis_modal.computing = None;
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                None
            }
            AppEvent::AnalysisCorrelationCompute => {
                if let Some(state) = &self.data_table_state {
                    if let Some(ref mut results) = self.analysis_modal.analysis_results {
                        let _ =
                            crate::statistics::compute_correlation_statistics(results, &state.lf);
                    }
                }
                self.analysis_modal.computing = None;
                self.busy = false;
                self.drain_keys_on_next_loop = true;
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
                    // History was already saved in TextInputEvent::Submit handler
                    self.input_mode = InputMode::Normal;
                    self.query_input.set_focused(false);
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
                self.busy = true;
                if let Some(state) = &mut self.data_table_state {
                    match state.pivot(spec) {
                        Ok(()) => {
                            self.pivot_melt_modal.close();
                            self.input_mode = InputMode::Normal;
                            Some(AppEvent::Collect)
                        }
                        Err(e) => {
                            self.busy = false;
                            self.error_modal
                                .show(crate::error_display::user_message_from_report(&e, None));
                            None
                        }
                    }
                } else {
                    self.busy = false;
                    None
                }
            }
            AppEvent::Melt(spec) => {
                self.busy = true;
                if let Some(state) = &mut self.data_table_state {
                    match state.melt(spec) {
                        Ok(()) => {
                            self.pivot_melt_modal.close();
                            self.input_mode = InputMode::Normal;
                            Some(AppEvent::Collect)
                        }
                        Err(e) => {
                            self.busy = false;
                            self.error_modal
                                .show(crate::error_display::user_message_from_report(&e, None));
                            None
                        }
                    }
                } else {
                    self.busy = false;
                    None
                }
            }
            AppEvent::ChartExport(path, format, title) => {
                self.busy = true;
                self.loading_state = LoadingState::Exporting {
                    file_path: path.clone(),
                    current_phase: "Exporting chart".to_string(),
                    progress_percent: 0,
                };
                Some(AppEvent::DoChartExport(
                    path.clone(),
                    *format,
                    title.clone(),
                ))
            }
            AppEvent::DoChartExport(path, format, title) => {
                let result = self.do_chart_export(path, *format, title);
                self.loading_state = LoadingState::Idle;
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                match result {
                    Ok(()) => {
                        self.success_modal.show(format!(
                            "Chart exported successfully to\n{}",
                            path.display()
                        ));
                        self.chart_export_modal.close();
                    }
                    Err(e) => {
                        self.error_modal
                            .show(crate::error_display::user_message_from_report(
                                &e,
                                Some(path),
                            ));
                        self.chart_export_modal.reopen_with_path(path, *format);
                    }
                }
                None
            }
            AppEvent::Export(path, format, options) => {
                if let Some(_state) = &self.data_table_state {
                    self.busy = true;
                    // Show progress immediately
                    self.loading_state = LoadingState::Exporting {
                        file_path: path.clone(),
                        current_phase: "Preparing export".to_string(),
                        progress_percent: 0,
                    };
                    // Return DoExport to allow UI to render progress before blocking
                    Some(AppEvent::DoExport(path.clone(), *format, options.clone()))
                } else {
                    None
                }
            }
            AppEvent::DoExport(path, format, options) => {
                if let Some(_state) = &self.data_table_state {
                    // Phase 1: show "Collecting data" so UI can redraw before blocking collect
                    self.loading_state = LoadingState::Exporting {
                        file_path: path.clone(),
                        current_phase: "Collecting data".to_string(),
                        progress_percent: 10,
                    };
                    Some(AppEvent::DoExportCollect(
                        path.clone(),
                        *format,
                        options.clone(),
                    ))
                } else {
                    self.busy = false;
                    None
                }
            }
            AppEvent::DoExportCollect(path, format, options) => {
                if let Some(state) = &self.data_table_state {
                    match state.lf.clone().collect() {
                        Ok(df) => {
                            self.export_df = Some(df);
                            let has_compression = match format {
                                ExportFormat::Csv => options.csv_compression.is_some(),
                                ExportFormat::Json => options.json_compression.is_some(),
                                ExportFormat::Ndjson => options.ndjson_compression.is_some(),
                                ExportFormat::Parquet | ExportFormat::Ipc | ExportFormat::Avro => {
                                    false
                                }
                            };
                            let phase = if has_compression {
                                "Writing and compressing file"
                            } else {
                                "Writing file"
                            };
                            self.loading_state = LoadingState::Exporting {
                                file_path: path.clone(),
                                current_phase: phase.to_string(),
                                progress_percent: 50,
                            };
                            Some(AppEvent::DoExportWrite(
                                path.clone(),
                                *format,
                                options.clone(),
                            ))
                        }
                        Err(e) => {
                            self.loading_state = LoadingState::Idle;
                            self.busy = false;
                            self.drain_keys_on_next_loop = true;
                            self.error_modal.show(format!(
                                "Export failed: {}",
                                crate::error_display::user_message_from_polars(&e)
                            ));
                            None
                        }
                    }
                } else {
                    self.busy = false;
                    None
                }
            }
            AppEvent::DoExportWrite(path, format, options) => {
                let result = self
                    .export_df
                    .take()
                    .map(|mut df| Self::export_data_from_df(&mut df, path, *format, options));
                self.loading_state = LoadingState::Idle;
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                match result {
                    Some(Ok(())) => {
                        self.success_modal
                            .show(format!("Data exported successfully to\n{}", path.display()));
                    }
                    Some(Err(e)) => {
                        let error_msg = Self::format_export_error(&e, path);
                        self.error_modal.show(error_msg);
                    }
                    None => {}
                }
                None
            }
            AppEvent::DoLoadParquetMetadata => {
                let path = self.path.clone();
                if let Some(p) = &path {
                    if let Some(meta) = read_parquet_metadata(p) {
                        self.parquet_metadata_cache = Some(meta);
                    }
                }
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                None
            }
            _ => None,
        }
    }

    /// Perform chart export to file. Exports what is currently visible (effective x + y).
    /// Title is optional; blank or whitespace means no chart title on export.
    fn do_chart_export(
        &self,
        path: &Path,
        format: ChartExportFormat,
        title: &str,
    ) -> color_eyre::Result<()> {
        let state = self
            .data_table_state
            .as_ref()
            .ok_or_else(|| color_eyre::eyre::eyre!("No data loaded"))?;
        let chart_title = title.trim();
        let chart_title = if chart_title.is_empty() {
            None
        } else {
            Some(chart_title.to_string())
        };

        match self.chart_modal.chart_kind {
            ChartKind::XY => {
                let x_column = self
                    .chart_modal
                    .effective_x_column()
                    .ok_or_else(|| color_eyre::eyre::eyre!("No X axis column selected"))?;
                let y_columns = self.chart_modal.effective_y_columns();
                if y_columns.is_empty() {
                    return Err(color_eyre::eyre::eyre!("No Y axis columns selected"));
                }

                let row_limit_opt = self.chart_modal.row_limit;
                let row_limit = self.chart_modal.effective_row_limit();
                let cache_matches = self.chart_cache.xy.as_ref().is_some_and(|c| {
                    c.x_column == *x_column
                        && c.y_columns == y_columns
                        && c.row_limit == row_limit_opt
                });

                let (series_vec, x_axis_kind_export, from_cache) = if cache_matches {
                    let cache = self.chart_cache.xy.as_ref().unwrap();
                    let pts = if self.chart_modal.log_scale {
                        cache.series_log.as_ref().cloned().unwrap_or_else(|| {
                            cache
                                .series
                                .iter()
                                .map(|s| s.iter().map(|&(x, y)| (x, y.max(0.0).ln_1p())).collect())
                                .collect()
                        })
                    } else {
                        cache.series.clone()
                    };
                    (pts, cache.x_axis_kind, true)
                } else {
                    let r = chart_data::prepare_chart_data(
                        &state.lf,
                        &state.schema,
                        x_column,
                        &y_columns,
                        row_limit,
                    )?;
                    (r.series, r.x_axis_kind, false)
                };

                let log_scale = self.chart_modal.log_scale;
                let series: Vec<ChartExportSeries> = series_vec
                    .iter()
                    .zip(y_columns.iter())
                    .filter(|(points, _)| !points.is_empty())
                    .map(|(points, name)| {
                        let pts = if log_scale && !from_cache {
                            points
                                .iter()
                                .map(|&(x, y)| (x, y.max(0.0).ln_1p()))
                                .collect()
                        } else {
                            points.clone()
                        };
                        ChartExportSeries {
                            name: name.clone(),
                            points: pts,
                        }
                    })
                    .collect();

                if series.is_empty() {
                    return Err(color_eyre::eyre::eyre!("No valid data points to export"));
                }

                let mut all_x_min = f64::INFINITY;
                let mut all_x_max = f64::NEG_INFINITY;
                let mut all_y_min = f64::INFINITY;
                let mut all_y_max = f64::NEG_INFINITY;
                for s in &series {
                    for &(x, y) in &s.points {
                        all_x_min = all_x_min.min(x);
                        all_x_max = all_x_max.max(x);
                        all_y_min = all_y_min.min(y);
                        all_y_max = all_y_max.max(y);
                    }
                }

                let chart_type = self.chart_modal.chart_type;
                let y_starts_at_zero = self.chart_modal.y_starts_at_zero;
                let y_min_bounds = if chart_type == ChartType::Bar {
                    0.0_f64.min(all_y_min)
                } else if y_starts_at_zero {
                    0.0
                } else {
                    all_y_min
                };
                let y_max_bounds = if all_y_max > y_min_bounds {
                    all_y_max
                } else {
                    y_min_bounds + 1.0
                };
                let x_min_bounds = if all_x_max > all_x_min {
                    all_x_min
                } else {
                    all_x_min - 0.5
                };
                let x_max_bounds = if all_x_max > all_x_min {
                    all_x_max
                } else {
                    all_x_min + 0.5
                };

                let x_label = x_column.to_string();
                let y_label = y_columns.join(", ");
                let bounds = ChartExportBounds {
                    x_min: x_min_bounds,
                    x_max: x_max_bounds,
                    y_min: y_min_bounds,
                    y_max: y_max_bounds,
                    x_label: x_label.clone(),
                    y_label: y_label.clone(),
                    x_axis_kind: x_axis_kind_export,
                    log_scale: self.chart_modal.log_scale,
                    chart_title,
                };

                match format {
                    ChartExportFormat::Png => write_chart_png(path, &series, chart_type, &bounds),
                    ChartExportFormat::Eps => write_chart_eps(path, &series, chart_type, &bounds),
                }
            }
            ChartKind::Histogram => {
                let column = self
                    .chart_modal
                    .effective_hist_column()
                    .ok_or_else(|| color_eyre::eyre::eyre!("No histogram column selected"))?;
                let row_limit = self.chart_modal.effective_row_limit();
                let data = if let Some(c) = self.chart_cache.histogram.as_ref().filter(|c| {
                    c.column == column
                        && c.bins == self.chart_modal.hist_bins
                        && c.row_limit == self.chart_modal.row_limit
                }) {
                    c.data.clone()
                } else {
                    chart_data::prepare_histogram_data(
                        &state.lf,
                        &column,
                        self.chart_modal.hist_bins,
                        row_limit,
                    )?
                };
                if data.bins.is_empty() {
                    return Err(color_eyre::eyre::eyre!("No valid data points to export"));
                }
                let points: Vec<(f64, f64)> =
                    data.bins.iter().map(|b| (b.center, b.count)).collect();
                let series = vec![ChartExportSeries {
                    name: column.clone(),
                    points,
                }];
                let x_max = if data.x_max > data.x_min {
                    data.x_max
                } else {
                    data.x_min + 1.0
                };
                let y_max = if data.max_count > 0.0 {
                    data.max_count
                } else {
                    1.0
                };
                let bounds = ChartExportBounds {
                    x_min: data.x_min,
                    x_max,
                    y_min: 0.0,
                    y_max,
                    x_label: column.clone(),
                    y_label: "Count".to_string(),
                    x_axis_kind: chart_data::XAxisTemporalKind::Numeric,
                    log_scale: false,
                    chart_title,
                };
                match format {
                    ChartExportFormat::Png => {
                        write_chart_png(path, &series, ChartType::Bar, &bounds)
                    }
                    ChartExportFormat::Eps => {
                        write_chart_eps(path, &series, ChartType::Bar, &bounds)
                    }
                }
            }
            ChartKind::BoxPlot => {
                let column = self
                    .chart_modal
                    .effective_box_column()
                    .ok_or_else(|| color_eyre::eyre::eyre!("No box plot column selected"))?;
                let row_limit = self.chart_modal.effective_row_limit();
                let data = if let Some(c) = self
                    .chart_cache
                    .box_plot
                    .as_ref()
                    .filter(|c| c.column == column && c.row_limit == self.chart_modal.row_limit)
                {
                    c.data.clone()
                } else {
                    chart_data::prepare_box_plot_data(
                        &state.lf,
                        std::slice::from_ref(&column),
                        row_limit,
                    )?
                };
                if data.stats.is_empty() {
                    return Err(color_eyre::eyre::eyre!("No valid data points to export"));
                }
                let bounds = BoxPlotExportBounds {
                    y_min: data.y_min,
                    y_max: data.y_max,
                    x_labels: vec![column.clone()],
                    x_label: "Columns".to_string(),
                    y_label: "Value".to_string(),
                    chart_title,
                };
                match format {
                    ChartExportFormat::Png => write_box_plot_png(path, &data, &bounds),
                    ChartExportFormat::Eps => write_box_plot_eps(path, &data, &bounds),
                }
            }
            ChartKind::Kde => {
                let column = self
                    .chart_modal
                    .effective_kde_column()
                    .ok_or_else(|| color_eyre::eyre::eyre!("No KDE column selected"))?;
                let row_limit = self.chart_modal.effective_row_limit();
                let data = if let Some(c) = self.chart_cache.kde.as_ref().filter(|c| {
                    c.column == column
                        && c.bandwidth_factor == self.chart_modal.kde_bandwidth_factor
                        && c.row_limit == self.chart_modal.row_limit
                }) {
                    c.data.clone()
                } else {
                    chart_data::prepare_kde_data(
                        &state.lf,
                        std::slice::from_ref(&column),
                        self.chart_modal.kde_bandwidth_factor,
                        row_limit,
                    )?
                };
                if data.series.is_empty() {
                    return Err(color_eyre::eyre::eyre!("No valid data points to export"));
                }
                let series: Vec<ChartExportSeries> = data
                    .series
                    .iter()
                    .map(|s| ChartExportSeries {
                        name: s.name.clone(),
                        points: s.points.clone(),
                    })
                    .collect();
                let bounds = ChartExportBounds {
                    x_min: data.x_min,
                    x_max: data.x_max,
                    y_min: 0.0,
                    y_max: data.y_max,
                    x_label: column.clone(),
                    y_label: "Density".to_string(),
                    x_axis_kind: chart_data::XAxisTemporalKind::Numeric,
                    log_scale: false,
                    chart_title,
                };
                match format {
                    ChartExportFormat::Png => {
                        write_chart_png(path, &series, ChartType::Line, &bounds)
                    }
                    ChartExportFormat::Eps => {
                        write_chart_eps(path, &series, ChartType::Line, &bounds)
                    }
                }
            }
            ChartKind::Heatmap => {
                let x_column = self
                    .chart_modal
                    .effective_heatmap_x_column()
                    .ok_or_else(|| color_eyre::eyre::eyre!("No heatmap X column selected"))?;
                let y_column = self
                    .chart_modal
                    .effective_heatmap_y_column()
                    .ok_or_else(|| color_eyre::eyre::eyre!("No heatmap Y column selected"))?;
                let row_limit = self.chart_modal.effective_row_limit();
                let data = if let Some(c) = self.chart_cache.heatmap.as_ref().filter(|c| {
                    c.x_column == *x_column
                        && c.y_column == *y_column
                        && c.bins == self.chart_modal.heatmap_bins
                        && c.row_limit == self.chart_modal.row_limit
                }) {
                    c.data.clone()
                } else {
                    chart_data::prepare_heatmap_data(
                        &state.lf,
                        &x_column,
                        &y_column,
                        self.chart_modal.heatmap_bins,
                        row_limit,
                    )?
                };
                if data.counts.is_empty() || data.max_count <= 0.0 {
                    return Err(color_eyre::eyre::eyre!("No valid data points to export"));
                }
                let bounds = ChartExportBounds {
                    x_min: data.x_min,
                    x_max: data.x_max,
                    y_min: data.y_min,
                    y_max: data.y_max,
                    x_label: x_column.clone(),
                    y_label: y_column.clone(),
                    x_axis_kind: chart_data::XAxisTemporalKind::Numeric,
                    log_scale: false,
                    chart_title,
                };
                match format {
                    ChartExportFormat::Png => write_heatmap_png(path, &data, &bounds),
                    ChartExportFormat::Eps => write_heatmap_eps(path, &data, &bounds),
                }
            }
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
                        return Err(color_eyre::eyre::eyre!(
                            "{}",
                            crate::error_display::user_message_from_polars(&error)
                        ));
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

            // Apply pivot or melt (reshape) if present. Order: query → filters → sort → reshape → column_order.
            if let Some(ref spec) = template.settings.pivot {
                if let Err(e) = state.pivot(spec) {
                    if let Some(saved) = saved_state {
                        self.restore_state(saved);
                    }
                    self.active_template_id = saved_active_template_id;
                    return Err(color_eyre::eyre::eyre!(
                        "{}",
                        crate::error_display::user_message_from_report(&e, None)
                    ));
                }
            } else if let Some(ref spec) = template.settings.melt {
                if let Err(e) = state.melt(spec) {
                    if let Some(saved) = saved_state {
                        self.restore_state(saved);
                    }
                    self.active_template_id = saved_active_template_id;
                    return Err(color_eyre::eyre::eyre!(
                        "{}",
                        crate::error_display::user_message_from_report(&e, None)
                    ));
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
            let _ = self.template_manager.save_template(&updated_template);
        }

        // Track active template
        self.active_template_id = Some(template.id.clone());

        Ok(())
    }

    /// Format export error messages to be more user-friendly using type-based handling.
    fn format_export_error(error: &color_eyre::eyre::Report, path: &Path) -> String {
        use std::io;

        for cause in error.chain() {
            if let Some(io_err) = cause.downcast_ref::<io::Error>() {
                let msg = crate::error_display::user_message_from_io(io_err, None);
                return format!("Cannot write to {}: {}", path.display(), msg);
            }
            if let Some(pe) = cause.downcast_ref::<polars::prelude::PolarsError>() {
                let msg = crate::error_display::user_message_from_polars(pe);
                return format!("Export failed: {}", msg);
            }
        }
        let error_str = error.to_string();
        let first_line = error_str.lines().next().unwrap_or("Unknown error").trim();
        format!("Export failed: {}", first_line)
    }

    /// Write an already-collected DataFrame to file. Used by two-phase export (DoExportWrite).
    fn export_data_from_df(
        df: &mut DataFrame,
        path: &Path,
        format: ExportFormat,
        options: &ExportOptions,
    ) -> Result<()> {
        use polars::prelude::*;
        use std::fs::File;
        use std::io::{BufWriter, Write};

        match format {
            ExportFormat::Csv => {
                use polars::prelude::CsvWriter;
                if let Some(compression) = options.csv_compression {
                    // Write to compressed file
                    let file = File::create(path)?;
                    let writer: Box<dyn Write> = match compression {
                        CompressionFormat::Gzip => Box::new(flate2::write::GzEncoder::new(
                            file,
                            flate2::Compression::default(),
                        )),
                        CompressionFormat::Zstd => {
                            Box::new(zstd::Encoder::new(file, 0)?.auto_finish())
                        }
                        CompressionFormat::Bzip2 => Box::new(bzip2::write::BzEncoder::new(
                            file,
                            bzip2::Compression::default(),
                        )),
                        CompressionFormat::Xz => {
                            Box::new(xz2::write::XzEncoder::new(
                                file, 6, // compression level
                            ))
                        }
                    };
                    CsvWriter::new(writer)
                        .with_separator(options.csv_delimiter)
                        .include_header(options.csv_include_header)
                        .finish(df)?;
                } else {
                    // Write uncompressed
                    let file = File::create(path)?;
                    CsvWriter::new(file)
                        .with_separator(options.csv_delimiter)
                        .include_header(options.csv_include_header)
                        .finish(df)?;
                }
            }
            ExportFormat::Parquet => {
                use polars::prelude::ParquetWriter;
                let file = File::create(path)?;
                let mut writer = BufWriter::new(file);
                ParquetWriter::new(&mut writer).finish(df)?;
            }
            ExportFormat::Json => {
                use polars::prelude::JsonWriter;
                if let Some(compression) = options.json_compression {
                    // Write to compressed file
                    let file = File::create(path)?;
                    let writer: Box<dyn Write> = match compression {
                        CompressionFormat::Gzip => Box::new(flate2::write::GzEncoder::new(
                            file,
                            flate2::Compression::default(),
                        )),
                        CompressionFormat::Zstd => {
                            Box::new(zstd::Encoder::new(file, 0)?.auto_finish())
                        }
                        CompressionFormat::Bzip2 => Box::new(bzip2::write::BzEncoder::new(
                            file,
                            bzip2::Compression::default(),
                        )),
                        CompressionFormat::Xz => {
                            Box::new(xz2::write::XzEncoder::new(
                                file, 6, // compression level
                            ))
                        }
                    };
                    JsonWriter::new(writer)
                        .with_json_format(JsonFormat::Json)
                        .finish(df)?;
                } else {
                    // Write uncompressed
                    let file = File::create(path)?;
                    JsonWriter::new(file)
                        .with_json_format(JsonFormat::Json)
                        .finish(df)?;
                }
            }
            ExportFormat::Ndjson => {
                use polars::prelude::{JsonFormat, JsonWriter};
                if let Some(compression) = options.ndjson_compression {
                    // Write to compressed file
                    let file = File::create(path)?;
                    let writer: Box<dyn Write> = match compression {
                        CompressionFormat::Gzip => Box::new(flate2::write::GzEncoder::new(
                            file,
                            flate2::Compression::default(),
                        )),
                        CompressionFormat::Zstd => {
                            Box::new(zstd::Encoder::new(file, 0)?.auto_finish())
                        }
                        CompressionFormat::Bzip2 => Box::new(bzip2::write::BzEncoder::new(
                            file,
                            bzip2::Compression::default(),
                        )),
                        CompressionFormat::Xz => {
                            Box::new(xz2::write::XzEncoder::new(
                                file, 6, // compression level
                            ))
                        }
                    };
                    JsonWriter::new(writer)
                        .with_json_format(JsonFormat::JsonLines)
                        .finish(df)?;
                } else {
                    // Write uncompressed
                    let file = File::create(path)?;
                    JsonWriter::new(file)
                        .with_json_format(JsonFormat::JsonLines)
                        .finish(df)?;
                }
            }
            ExportFormat::Ipc => {
                use polars::prelude::IpcWriter;
                let file = File::create(path)?;
                let mut writer = BufWriter::new(file);
                IpcWriter::new(&mut writer).finish(df)?;
            }
            ExportFormat::Avro => {
                use polars::io::avro::AvroWriter;
                let file = File::create(path)?;
                let mut writer = BufWriter::new(file);
                AvroWriter::new(&mut writer).finish(df)?;
            }
        }

        Ok(())
    }

    #[allow(dead_code)] // Used only when not using two-phase export; kept for tests/single-shot use
    fn export_data(
        state: &DataTableState,
        path: &Path,
        format: ExportFormat,
        options: &ExportOptions,
    ) -> Result<()> {
        let mut df = state.lf.clone().collect()?;
        Self::export_data_from_df(&mut df, path, format, options)
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
                pivot: state.last_pivot_spec().cloned(),
                melt: state.last_melt_spec().cloned(),
            }
        } else {
            template::TemplateSettings {
                query: None,
                filters: Vec::new(),
                sort_columns: Vec::new(),
                sort_ascending: true,
                column_order: Vec::new(),
                locked_columns_count: 0,
                pivot: None,
                melt: None,
            }
        };

        self.template_manager
            .create_template(name, description, match_criteria, settings)
    }

    fn get_help_info(&self) -> (String, String) {
        let (title, content) = match self.input_mode {
            InputMode::Normal => ("Main View Help", help_strings::main_view()),
            InputMode::Editing => match self.input_type {
                Some(InputType::Search) => ("Query Help", help_strings::query()),
                _ => ("Editing Help", help_strings::editing()),
            },
            InputMode::SortFilter => ("Sort & Filter Help", help_strings::sort_filter()),
            InputMode::PivotMelt => ("Pivot / Melt Help", help_strings::pivot_melt()),
            InputMode::Export => ("Export Help", help_strings::export()),
            InputMode::Info => ("Info Panel Help", help_strings::info_panel()),
            InputMode::Chart => ("Chart Help", help_strings::chart()),
        };
        (title.to_string(), content.to_string())
    }
}

impl Widget for &mut App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        self.debug.num_frames += 1;
        if self.debug.enabled {
            self.debug.show_help_at_render = self.show_help;
        }

        // Clear entire area first so no ghost text from any widget (loading gauge label,
        // modals, controls, etc.) can persist when layout or visibility changes (e.g. after pivot).
        Clear.render(area, buf);

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
                err_msg = crate::error_display::user_message_from_polars(e);
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
        // Clear entire main content so no ghost text from modals or previous layout persists (e.g. after pivot).
        Clear.render(main_area, buf);
        let mut data_area = main_area;
        let mut sort_area = Rect::default();

        if self.sort_filter_modal.active {
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
        if self.info_modal.active {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Min(0), Constraint::Max(72)])
                .split(main_area);
            data_area = chunks[0];
            sort_area = chunks[1];
        }

        // Extract colors and table config before mutable borrow to avoid borrow checker issues
        let primary_color = self.color("keybind_hints");
        let _controls_bg_color = self.color("controls_bg");
        let table_header_color = self.color("table_header");
        let row_numbers_color = self.color("row_numbers");
        let column_separator_color = self.color("column_separator");
        let table_header_bg_color = self.color("table_header_bg");
        let modal_border_color = self.color("modal_border");
        let info_active_color = self.color("modal_border_active");
        let info_primary_color = self.color("text_primary");
        let table_cell_padding = self.table_cell_padding;
        let alternate_row_bg = self.theme.get_optional("alternate_row_color");
        let column_colors = self.column_colors;
        let (str_col, int_col, float_col, bool_col, temporal_col) = if column_colors {
            (
                self.theme.get("str_col"),
                self.theme.get("int_col"),
                self.theme.get("float_col"),
                self.theme.get("bool_col"),
                self.theme.get("temporal_col"),
            )
        } else {
            (
                Color::Reset,
                Color::Reset,
                Color::Reset,
                Color::Reset,
                Color::Reset,
            )
        };

        // Parquet metadata is loaded via DoLoadParquetMetadata when info panel is opened (not in render)

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
                            .border_type(BorderType::Rounded)
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

                Clear.render(table_area, buf);
                let mut dt = DataTable::new()
                    .with_colors(
                        table_header_bg_color,
                        table_header_color,
                        row_numbers_color,
                        column_separator_color,
                    )
                    .with_cell_padding(table_cell_padding)
                    .with_alternate_row_bg(alternate_row_bg);
                if column_colors {
                    dt = dt.with_column_type_colors(
                        str_col,
                        int_col,
                        float_col,
                        bool_col,
                        temporal_col,
                    );
                }
                dt.render(table_area, buf, state);
                if self.info_modal.active {
                    let ctx = InfoContext {
                        path: self.path.as_deref(),
                        format: self.original_file_format,
                        parquet_metadata: self.parquet_metadata_cache.as_ref(),
                    };
                    let mut info_widget = DataTableInfo::new(
                        state,
                        ctx,
                        &mut self.info_modal,
                        modal_border_color,
                        info_active_color,
                        info_primary_color,
                    );
                    info_widget.render(sort_area, buf);
                }
            }
            None => {
                Paragraph::new("No data loaded").render(layout[0], buf);
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
                Some(InputType::GoToLine) => "Go to line",
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
                .border_type(BorderType::Rounded)
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

                // Render input using TextInput widget
                (&self.query_input).render(chunks[0], buf);
                Paragraph::new(err_msg)
                    .style(Style::default().fg(self.color("error")))
                    .wrap(ratatui::widgets::Wrap { trim: true })
                    .render(chunks[2], buf);
            } else {
                // Render input using TextInput widget
                (&self.query_input).render(inner_area, buf);
            }
        }

        if self.sort_filter_modal.active {
            Clear.render(sort_area, buf);
            let block = Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .title("Sort & Filter");
            let inner_area = block.inner(sort_area);
            block.render(sort_area, buf);

            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(2), // Tab bar + line
                    Constraint::Min(0),    // Body
                    Constraint::Length(3), // Footer
                ])
                .split(inner_area);

            // Tab bar + line
            let tab_line_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(1), Constraint::Length(1)])
                .split(chunks[0]);
            let tab_selected = match self.sort_filter_modal.active_tab {
                SortFilterTab::Sort => 0,
                SortFilterTab::Filter => 1,
            };
            let border_c = self.color("modal_border");
            let active_c = self.color("modal_border_active");
            let tabs = Tabs::new(vec!["Sort", "Filter"])
                .style(Style::default().fg(border_c))
                .highlight_style(
                    Style::default()
                        .fg(active_c)
                        .add_modifier(Modifier::REVERSED),
                )
                .select(tab_selected);
            tabs.render(tab_line_chunks[0], buf);
            let line_style = if self.sort_filter_modal.focus == SortFilterFocus::TabBar {
                Style::default().fg(active_c)
            } else {
                Style::default().fg(border_c)
            };
            Block::default()
                .borders(Borders::BOTTOM)
                .border_type(BorderType::Rounded)
                .border_style(line_style)
                .render(tab_line_chunks[1], buf);

            if self.sort_filter_modal.active_tab == SortFilterTab::Filter {
                let fchunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(3),
                        Constraint::Length(3),
                        Constraint::Min(0),
                    ])
                    .split(chunks[1]);

                let row_layout = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints([
                        Constraint::Percentage(30),
                        Constraint::Percentage(20),
                        Constraint::Percentage(30),
                        Constraint::Percentage(20),
                    ])
                    .split(fchunks[0]);

                let col_name = if self.sort_filter_modal.filter.available_columns.is_empty() {
                    ""
                } else {
                    &self.sort_filter_modal.filter.available_columns
                        [self.sort_filter_modal.filter.new_column_idx]
                };
                let col_style = if self.sort_filter_modal.filter.focus == FilterFocus::Column {
                    Style::default().fg(active_c)
                } else {
                    Style::default().fg(border_c)
                };
                Paragraph::new(col_name)
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .border_type(BorderType::Rounded)
                            .title("Col")
                            .border_style(col_style),
                    )
                    .render(row_layout[0], buf);

                let op_name = FilterOperator::iterator()
                    .nth(self.sort_filter_modal.filter.new_operator_idx)
                    .unwrap()
                    .as_str();
                let op_style = if self.sort_filter_modal.filter.focus == FilterFocus::Operator {
                    Style::default().fg(active_c)
                } else {
                    Style::default().fg(border_c)
                };
                Paragraph::new(op_name)
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .border_type(BorderType::Rounded)
                            .title("Op")
                            .border_style(op_style),
                    )
                    .render(row_layout[1], buf);

                let val_style = if self.sort_filter_modal.filter.focus == FilterFocus::Value {
                    Style::default().fg(active_c)
                } else {
                    Style::default().fg(border_c)
                };
                Paragraph::new(self.sort_filter_modal.filter.new_value.as_str())
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .border_type(BorderType::Rounded)
                            .title("Val")
                            .border_style(val_style),
                    )
                    .render(row_layout[2], buf);

                let log_name = LogicalOperator::iterator()
                    .nth(self.sort_filter_modal.filter.new_logical_idx)
                    .unwrap()
                    .as_str();
                let log_style = if self.sort_filter_modal.filter.focus == FilterFocus::Logical {
                    Style::default().fg(active_c)
                } else {
                    Style::default().fg(border_c)
                };
                Paragraph::new(log_name)
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .border_type(BorderType::Rounded)
                            .title("Logic")
                            .border_style(log_style),
                    )
                    .render(row_layout[3], buf);

                let add_style = if self.sort_filter_modal.filter.focus == FilterFocus::Add {
                    Style::default().fg(active_c)
                } else {
                    Style::default().fg(border_c)
                };
                Paragraph::new("Add Filter")
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .border_type(BorderType::Rounded)
                            .border_style(add_style),
                    )
                    .centered()
                    .render(fchunks[1], buf);

                let items: Vec<ListItem> = self
                    .sort_filter_modal
                    .filter
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
                let list_style = if self.sort_filter_modal.filter.focus == FilterFocus::Statements {
                    Style::default().fg(active_c)
                } else {
                    Style::default().fg(border_c)
                };
                let list = List::new(items)
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .border_type(BorderType::Rounded)
                            .title("Current Filters")
                            .border_style(list_style),
                    )
                    .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
                StatefulWidget::render(
                    list,
                    fchunks[2],
                    buf,
                    &mut self.sort_filter_modal.filter.list_state,
                );
            } else {
                // Sort tab body
                let schunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(3),
                        Constraint::Min(0),
                        Constraint::Length(2),
                        Constraint::Length(3),
                    ])
                    .split(chunks[1]);

                let filter_block_title = "Filter Columns";
                let mut filter_block_border_style = Style::default().fg(border_c);
                if self.sort_filter_modal.sort.focus == SortFocus::Filter {
                    filter_block_border_style = filter_block_border_style.fg(active_c);
                }
                let filter_block = Block::default()
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded)
                    .title(filter_block_title)
                    .border_style(filter_block_border_style);
                let filter_inner_area = filter_block.inner(schunks[0]);
                filter_block.render(schunks[0], buf);

                // Render filter input using TextInput widget
                let is_focused = self.sort_filter_modal.sort.focus == SortFocus::Filter;
                self.sort_filter_modal
                    .sort
                    .filter_input
                    .set_focused(is_focused);
                (&self.sort_filter_modal.sort.filter_input).render(filter_inner_area, buf);

                let filtered = self.sort_filter_modal.sort.filtered_columns();
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
                    Cell::from("🔒").style(Style::default()),
                    Cell::from("Order").style(Style::default()),
                    Cell::from("Sort").style(Style::default()),
                    Cell::from("Name").style(Style::default()),
                ])
                .style(Style::default().add_modifier(Modifier::UNDERLINED));

                let table_border_style =
                    if self.sort_filter_modal.sort.focus == SortFocus::ColumnList {
                        Style::default().fg(active_c)
                    } else {
                        Style::default().fg(border_c)
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
                        .border_type(BorderType::Rounded)
                        .title("Columns")
                        .border_style(table_border_style),
                )
                .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

                StatefulWidget::render(
                    table,
                    schunks[1],
                    buf,
                    &mut self.sort_filter_modal.sort.table_state,
                );

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

                Paragraph::new(vec![hint_line1, hint_line2]).render(schunks[2], buf);

                let order_border_style = if self.sort_filter_modal.sort.focus == SortFocus::Order {
                    Style::default().fg(active_c)
                } else {
                    Style::default().fg(border_c)
                };

                let order_block = Block::default()
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded)
                    .title("Order")
                    .border_style(order_border_style);
                let order_inner = order_block.inner(schunks[3]);
                order_block.render(schunks[3], buf);

                let order_layout = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                    .split(order_inner);

                // Ascending option
                let ascending_indicator = if self.sort_filter_modal.sort.ascending {
                    "●"
                } else {
                    "○"
                };
                let ascending_text = format!("{} Ascending", ascending_indicator);
                let ascending_style = if self.sort_filter_modal.sort.ascending {
                    Style::default().add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                Paragraph::new(ascending_text)
                    .style(ascending_style)
                    .centered()
                    .render(order_layout[0], buf);

                // Descending option
                let descending_indicator = if !self.sort_filter_modal.sort.ascending {
                    "●"
                } else {
                    "○"
                };
                let descending_text = format!("{} Descending", descending_indicator);
                let descending_style = if !self.sort_filter_modal.sort.ascending {
                    Style::default().add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                Paragraph::new(descending_text)
                    .style(descending_style)
                    .centered()
                    .render(order_layout[1], buf);
            }

            // Shared footer
            let footer_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(33),
                    Constraint::Percentage(33),
                    Constraint::Percentage(34),
                ])
                .split(chunks[2]);

            let mut apply_text_style = Style::default();
            let mut apply_border_style = Style::default();
            if self.sort_filter_modal.focus == SortFilterFocus::Apply {
                apply_text_style = apply_text_style.fg(active_c);
                apply_border_style = apply_border_style.fg(active_c);
            } else {
                apply_text_style = apply_text_style.fg(border_c);
                apply_border_style = apply_border_style.fg(border_c);
            }
            if self.sort_filter_modal.active_tab == SortFilterTab::Sort
                && self.sort_filter_modal.sort.has_unapplied_changes
            {
                apply_text_style = apply_text_style.add_modifier(Modifier::BOLD);
            }

            Paragraph::new("Apply")
                .style(apply_text_style)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .border_style(apply_border_style),
                )
                .centered()
                .render(footer_chunks[0], buf);

            let cancel_style = if self.sort_filter_modal.focus == SortFilterFocus::Cancel {
                Style::default().fg(active_c)
            } else {
                Style::default().fg(border_c)
            };
            Paragraph::new("Cancel")
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .border_style(cancel_style),
                )
                .centered()
                .render(footer_chunks[1], buf);

            let clear_style = if self.sort_filter_modal.focus == SortFilterFocus::Clear {
                Style::default().fg(active_c)
            } else {
                Style::default().fg(border_c)
            };
            Paragraph::new("Clear")
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .border_style(clear_style),
                )
                .centered()
                .render(footer_chunks[2], buf);
        }

        if self.template_modal.active {
            Clear.render(sort_area, buf);
            let modal_title = match self.template_modal.mode {
                TemplateModalMode::List => "Templates",
                TemplateModalMode::Create => "Create Template",
                TemplateModalMode::Edit => "Edit Template",
            };
            let block = Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .title(modal_title);
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
                        Cell::from("●").style(Style::default()),
                        Cell::from(" ").style(Style::default()), // Active column header (empty)
                        Cell::from("Name").style(Style::default()),
                        Cell::from("Description").style(Style::default()),
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
                            .border_type(BorderType::Rounded)
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
                        .border_type(BorderType::Rounded)
                        .title(name_title)
                        .title_style(if self.template_modal.name_error.is_some() {
                            Style::default().fg(self.color("error"))
                        } else {
                            Style::default().add_modifier(Modifier::BOLD)
                        })
                        .border_style(name_style);
                    let name_inner = name_block.inner(chunks[0]);
                    name_block.render(chunks[0], buf);
                    // Render name input using TextInput widget
                    let is_focused = self.template_modal.create_focus == CreateFocus::Name;
                    self.template_modal
                        .create_name_input
                        .set_focused(is_focused);
                    (&self.template_modal.create_name_input).render(name_inner, buf);

                    // Description input (scrollable, multi-line)
                    let desc_style = if self.template_modal.create_focus == CreateFocus::Description
                    {
                        Style::default().fg(self.color("modal_border_active"))
                    } else {
                        Style::default()
                    };
                    let desc_block = Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .title("Description")
                        .border_style(desc_style);
                    let desc_inner = desc_block.inner(chunks[1]);
                    desc_block.render(chunks[1], buf);

                    // Render description input using MultiLineTextInput widget
                    let is_focused = self.template_modal.create_focus == CreateFocus::Description;
                    self.template_modal
                        .create_description_input
                        .set_focused(is_focused);
                    // Auto-scroll to keep cursor visible
                    self.template_modal
                        .create_description_input
                        .ensure_cursor_visible(desc_inner.height, desc_inner.width);
                    (&self.template_modal.create_description_input).render(desc_inner, buf);

                    // Exact Path
                    let exact_path_style =
                        if self.template_modal.create_focus == CreateFocus::ExactPath {
                            Style::default().fg(self.color("modal_border_active"))
                        } else {
                            Style::default()
                        };
                    let exact_path_block = Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .title("Exact Path")
                        .border_style(exact_path_style);
                    let exact_path_inner = exact_path_block.inner(chunks[2]);
                    exact_path_block.render(chunks[2], buf);
                    // Render exact path input using TextInput widget
                    let is_focused = self.template_modal.create_focus == CreateFocus::ExactPath;
                    self.template_modal
                        .create_exact_path_input
                        .set_focused(is_focused);
                    (&self.template_modal.create_exact_path_input).render(exact_path_inner, buf);

                    // Relative Path
                    let relative_path_style =
                        if self.template_modal.create_focus == CreateFocus::RelativePath {
                            Style::default().fg(self.color("modal_border_active"))
                        } else {
                            Style::default()
                        };
                    let relative_path_block = Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .title("Relative Path")
                        .border_style(relative_path_style);
                    let relative_path_inner = relative_path_block.inner(chunks[3]);
                    relative_path_block.render(chunks[3], buf);
                    // Render relative path input using TextInput widget
                    let is_focused = self.template_modal.create_focus == CreateFocus::RelativePath;
                    self.template_modal
                        .create_relative_path_input
                        .set_focused(is_focused);
                    (&self.template_modal.create_relative_path_input)
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
                        .border_type(BorderType::Rounded)
                        .title("Path Pattern")
                        .border_style(path_pattern_style);
                    let path_pattern_inner = path_pattern_block.inner(chunks[4]);
                    path_pattern_block.render(chunks[4], buf);
                    // Render path pattern input using TextInput widget
                    let is_focused = self.template_modal.create_focus == CreateFocus::PathPattern;
                    self.template_modal
                        .create_path_pattern_input
                        .set_focused(is_focused);
                    (&self.template_modal.create_path_pattern_input)
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
                        .border_type(BorderType::Rounded)
                        .title("Filename Pattern")
                        .border_style(filename_pattern_style);
                    let filename_pattern_inner = filename_pattern_block.inner(chunks[5]);
                    filename_pattern_block.render(chunks[5], buf);
                    // Render filename pattern input using TextInput widget
                    let is_focused =
                        self.template_modal.create_focus == CreateFocus::FilenamePattern;
                    self.template_modal
                        .create_filename_pattern_input
                        .set_focused(is_focused);
                    (&self.template_modal.create_filename_pattern_input)
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
                                .border_type(BorderType::Rounded)
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
                                .border_type(BorderType::Rounded)
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
                                .border_type(BorderType::Rounded)
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
                        .border_type(BorderType::Rounded)
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
                                .border_type(BorderType::Rounded)
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
                                .border_type(BorderType::Rounded)
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
                                .border_type(BorderType::Rounded)
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

        if self.export_modal.active {
            let border = self.color("modal_border");
            let active = self.color("modal_border_active");
            let text_primary = self.color("text_primary");
            let text_inverse = self.color("text_inverse");
            // Center the modal
            let modal_width = (area.width * 3 / 4).min(80);
            let modal_height = 20;
            let modal_x = (area.width.saturating_sub(modal_width)) / 2;
            let modal_y = (area.height.saturating_sub(modal_height)) / 2;
            let modal_area = Rect {
                x: modal_x,
                y: modal_y,
                width: modal_width,
                height: modal_height,
            };
            export::render_export_modal(
                modal_area,
                buf,
                &mut self.export_modal,
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

            // Progress overlay when chunked describe is running
            if let Some(ref progress) = self.analysis_modal.computing {
                let border = self.color("modal_border");
                let text_primary = self.color("text_primary");
                let label = self.color("label");
                let percent = if progress.total > 0 {
                    (progress.current as u16).saturating_mul(100) / progress.total as u16
                } else {
                    0
                };
                Clear.render(analysis_area, buf);
                let block = Block::default()
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded)
                    .border_style(Style::default().fg(border))
                    .title(" Analysis ");
                let inner = block.inner(analysis_area);
                block.render(analysis_area, buf);
                let text = format!(
                    "{}: {} / {}",
                    progress.phase, progress.current, progress.total
                );
                Paragraph::new(text)
                    .style(Style::default().fg(text_primary))
                    .render(
                        Rect {
                            x: inner.x,
                            y: inner.y,
                            width: inner.width,
                            height: 1,
                        },
                        buf,
                    );
                Gauge::default()
                    .gauge_style(Style::default().fg(label))
                    .ratio(percent as f64 / 100.0)
                    .render(
                        Rect {
                            x: inner.x,
                            y: inner.y + 1,
                            width: inner.width,
                            height: 1,
                        },
                        buf,
                    );
            } else if let Some(state) = &self.data_table_state {
                // Only run sync recompute when user has selected a tool (Describe triggers chunked path on Enter;
                // this is fallback for seed change or when we have no results after selecting a tool).
                let selected_tool = self.analysis_modal.selected_tool;
                let needs_recompute = selected_tool.is_some()
                    && (self.analysis_modal.analysis_results.is_none()
                        || self
                            .analysis_modal
                            .analysis_results
                            .as_ref()
                            .map(|r| r.sample_seed != self.analysis_modal.random_seed)
                            .unwrap_or(true));

                if needs_recompute {
                    self.busy = true;
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
                        self.sampling_threshold,
                        self.analysis_modal.random_seed,
                        options,
                    ) {
                        Ok(results) => {
                            self.analysis_modal.analysis_results = Some(results);
                        }
                        Err(e) => {
                            // Still render the modal with error message
                            Clear.render(analysis_area, buf);
                            let error_msg = format!(
                                "Error computing statistics: {}",
                                crate::error_display::user_message_from_report(&e, None)
                            );
                            Paragraph::new(error_msg)
                                .centered()
                                .style(Style::default().fg(self.color("error")))
                                .render(analysis_area, buf);
                            // Don't return - continue to render toolbar
                        }
                    }
                    self.busy = false;
                    self.drain_keys_on_next_loop = true;
                }

                // Distribution and Correlation are computed via deferred events (AnalysisDistributionCompute
                // / AnalysisCorrelationCompute) when the user selects that tool, so progress overlay and throbber show first.

                // Always render the analysis widget when we have data (with or without results: widget shows
                // "Select an analysis tool", "Computing...", or the selected tool content).
                let context = state.get_analysis_context();
                Clear.render(analysis_area, buf);
                let column_offset = match self.analysis_modal.selected_tool {
                    Some(analysis_modal::AnalysisTool::Describe) => {
                        self.analysis_modal.describe_column_offset
                    }
                    Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                        self.analysis_modal.distribution_column_offset
                    }
                    Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                        self.analysis_modal.correlation_column_offset
                    }
                    None => 0,
                };

                let config = widgets::analysis::AnalysisWidgetConfig {
                    state,
                    results: self.analysis_modal.analysis_results.as_ref(),
                    context: &context,
                    view: self.analysis_modal.view,
                    selected_tool: self.analysis_modal.selected_tool,
                    column_offset,
                    selected_correlation: self.analysis_modal.selected_correlation,
                    focus: self.analysis_modal.focus,
                    selected_theoretical_distribution: self
                        .analysis_modal
                        .selected_theoretical_distribution,
                    histogram_scale: self.analysis_modal.histogram_scale,
                    theme: &self.theme,
                    table_cell_padding: self.table_cell_padding,
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
                // No data available
                Clear.render(analysis_area, buf);
                Paragraph::new("No data available for analysis")
                    .centered()
                    .style(Style::default().fg(self.color("warning")))
                    .render(analysis_area, buf);
            }
            // Don't return - continue to render toolbar and other UI elements
        }

        // Render chart view (full screen in main area)
        if self.input_mode == InputMode::Chart {
            let chart_area = main_area;
            Clear.render(chart_area, buf);
            let mut xy_series: Option<&Vec<Vec<(f64, f64)>>> = None;
            let mut x_axis_kind = chart_data::XAxisTemporalKind::Numeric;
            let mut x_bounds: Option<(f64, f64)> = None;
            let mut hist_data: Option<&chart_data::HistogramData> = None;
            let mut box_data: Option<&chart_data::BoxPlotData> = None;
            let mut kde_data: Option<&chart_data::KdeData> = None;
            let mut heatmap_data: Option<&chart_data::HeatmapData> = None;

            let row_limit_opt = self.chart_modal.row_limit;
            let row_limit = self.chart_modal.effective_row_limit();
            match self.chart_modal.chart_kind {
                ChartKind::XY => {
                    if let Some(x_column) = self.chart_modal.effective_x_column() {
                        let x_key = x_column.to_string();
                        let y_columns = self.chart_modal.effective_y_columns();
                        if !y_columns.is_empty() {
                            let use_cache = self.chart_cache.xy.as_ref().filter(|c| {
                                c.x_column == x_key
                                    && c.y_columns == y_columns
                                    && c.row_limit == row_limit_opt
                            });
                            if use_cache.is_none() {
                                if let Some(state) = self.data_table_state.as_ref() {
                                    if let Ok(result) = chart_data::prepare_chart_data(
                                        &state.lf,
                                        &state.schema,
                                        x_column,
                                        &y_columns,
                                        row_limit,
                                    ) {
                                        self.chart_cache.xy = Some(ChartCacheXY {
                                            x_column: x_key.clone(),
                                            y_columns: y_columns.clone(),
                                            row_limit: row_limit_opt,
                                            series: result.series,
                                            series_log: None,
                                            x_axis_kind: result.x_axis_kind,
                                        });
                                    }
                                }
                            }
                            if self.chart_modal.log_scale {
                                if let Some(cache) = self.chart_cache.xy.as_mut() {
                                    if cache.x_column == x_key
                                        && cache.y_columns == y_columns
                                        && cache.row_limit == row_limit_opt
                                        && cache.series_log.is_none()
                                        && cache.series.iter().any(|s| !s.is_empty())
                                    {
                                        cache.series_log = Some(
                                            cache
                                                .series
                                                .iter()
                                                .map(|pts| {
                                                    pts.iter()
                                                        .map(|&(x, y)| (x, y.max(0.0).ln_1p()))
                                                        .collect()
                                                })
                                                .collect(),
                                        );
                                    }
                                }
                            }
                            if let Some(cache) = self.chart_cache.xy.as_ref() {
                                if cache.x_column == x_key
                                    && cache.y_columns == y_columns
                                    && cache.row_limit == row_limit_opt
                                {
                                    x_axis_kind = cache.x_axis_kind;
                                    if self.chart_modal.log_scale {
                                        if let Some(ref log) = cache.series_log {
                                            if log.iter().any(|v| !v.is_empty()) {
                                                xy_series = Some(log);
                                            }
                                        }
                                    } else if cache.series.iter().any(|s| !s.is_empty()) {
                                        xy_series = Some(&cache.series);
                                    }
                                }
                            }
                        } else {
                            // Only X selected: cache x range for axis bounds
                            let use_cache =
                                self.chart_cache.x_range.as_ref().filter(|c| {
                                    c.x_column == x_key && c.row_limit == row_limit_opt
                                });
                            if use_cache.is_none() {
                                if let Some(state) = self.data_table_state.as_ref() {
                                    if let Ok(result) = chart_data::prepare_chart_x_range(
                                        &state.lf,
                                        &state.schema,
                                        x_column,
                                        row_limit,
                                    ) {
                                        self.chart_cache.x_range = Some(ChartCacheXRange {
                                            x_column: x_key.clone(),
                                            row_limit: row_limit_opt,
                                            x_min: result.x_min,
                                            x_max: result.x_max,
                                            x_axis_kind: result.x_axis_kind,
                                        });
                                    }
                                }
                            }
                            if let Some(cache) = self.chart_cache.x_range.as_ref() {
                                if cache.x_column == x_key && cache.row_limit == row_limit_opt {
                                    x_axis_kind = cache.x_axis_kind;
                                    x_bounds = Some((cache.x_min, cache.x_max));
                                }
                            } else if let Some(state) = self.data_table_state.as_ref() {
                                x_axis_kind = chart_data::x_axis_temporal_kind_for_column(
                                    &state.schema,
                                    x_column,
                                );
                            }
                        }
                    }
                }
                ChartKind::Histogram => {
                    if let (Some(state), Some(column)) = (
                        self.data_table_state.as_ref(),
                        self.chart_modal.effective_hist_column(),
                    ) {
                        let bins = self.chart_modal.hist_bins;
                        let use_cache = self.chart_cache.histogram.as_ref().filter(|c| {
                            c.column == column && c.bins == bins && c.row_limit == row_limit_opt
                        });
                        if use_cache.is_none() {
                            if let Ok(data) = chart_data::prepare_histogram_data(
                                &state.lf, &column, bins, row_limit,
                            ) {
                                self.chart_cache.histogram = Some(ChartCacheHistogram {
                                    column: column.clone(),
                                    bins,
                                    row_limit: row_limit_opt,
                                    data,
                                });
                            }
                        }
                        hist_data = self
                            .chart_cache
                            .histogram
                            .as_ref()
                            .filter(|c| {
                                c.column == column && c.bins == bins && c.row_limit == row_limit_opt
                            })
                            .map(|c| &c.data);
                    }
                }
                ChartKind::BoxPlot => {
                    if let (Some(state), Some(column)) = (
                        self.data_table_state.as_ref(),
                        self.chart_modal.effective_box_column(),
                    ) {
                        let use_cache = self
                            .chart_cache
                            .box_plot
                            .as_ref()
                            .filter(|c| c.column == column && c.row_limit == row_limit_opt);
                        if use_cache.is_none() {
                            if let Ok(data) = chart_data::prepare_box_plot_data(
                                &state.lf,
                                std::slice::from_ref(&column),
                                row_limit,
                            ) {
                                self.chart_cache.box_plot = Some(ChartCacheBoxPlot {
                                    column: column.clone(),
                                    row_limit: row_limit_opt,
                                    data,
                                });
                            }
                        }
                        box_data = self
                            .chart_cache
                            .box_plot
                            .as_ref()
                            .filter(|c| c.column == column && c.row_limit == row_limit_opt)
                            .map(|c| &c.data);
                    }
                }
                ChartKind::Kde => {
                    if let (Some(state), Some(column)) = (
                        self.data_table_state.as_ref(),
                        self.chart_modal.effective_kde_column(),
                    ) {
                        let bandwidth = self.chart_modal.kde_bandwidth_factor;
                        let use_cache = self.chart_cache.kde.as_ref().filter(|c| {
                            c.column == column
                                && c.bandwidth_factor == bandwidth
                                && c.row_limit == row_limit_opt
                        });
                        if use_cache.is_none() {
                            if let Ok(data) = chart_data::prepare_kde_data(
                                &state.lf,
                                std::slice::from_ref(&column),
                                bandwidth,
                                row_limit,
                            ) {
                                self.chart_cache.kde = Some(ChartCacheKde {
                                    column: column.clone(),
                                    bandwidth_factor: bandwidth,
                                    row_limit: row_limit_opt,
                                    data,
                                });
                            }
                        }
                        kde_data = self
                            .chart_cache
                            .kde
                            .as_ref()
                            .filter(|c| {
                                c.column == column
                                    && c.bandwidth_factor == bandwidth
                                    && c.row_limit == row_limit_opt
                            })
                            .map(|c| &c.data);
                    }
                }
                ChartKind::Heatmap => {
                    if let (Some(state), Some(x_column), Some(y_column)) = (
                        self.data_table_state.as_ref(),
                        self.chart_modal.effective_heatmap_x_column(),
                        self.chart_modal.effective_heatmap_y_column(),
                    ) {
                        let bins = self.chart_modal.heatmap_bins;
                        let use_cache = self.chart_cache.heatmap.as_ref().filter(|c| {
                            c.x_column == x_column
                                && c.y_column == y_column
                                && c.bins == bins
                                && c.row_limit == row_limit_opt
                        });
                        if use_cache.is_none() {
                            if let Ok(data) = chart_data::prepare_heatmap_data(
                                &state.lf, &x_column, &y_column, bins, row_limit,
                            ) {
                                self.chart_cache.heatmap = Some(ChartCacheHeatmap {
                                    x_column: x_column.clone(),
                                    y_column: y_column.clone(),
                                    bins,
                                    row_limit: row_limit_opt,
                                    data,
                                });
                            }
                        }
                        heatmap_data = self
                            .chart_cache
                            .heatmap
                            .as_ref()
                            .filter(|c| {
                                c.x_column == x_column
                                    && c.y_column == y_column
                                    && c.bins == bins
                                    && c.row_limit == row_limit_opt
                            })
                            .map(|c| &c.data);
                    }
                }
            }

            let render_data = match self.chart_modal.chart_kind {
                ChartKind::XY => widgets::chart::ChartRenderData::XY {
                    series: xy_series,
                    x_axis_kind,
                    x_bounds,
                },
                ChartKind::Histogram => {
                    widgets::chart::ChartRenderData::Histogram { data: hist_data }
                }
                ChartKind::BoxPlot => widgets::chart::ChartRenderData::BoxPlot { data: box_data },
                ChartKind::Kde => widgets::chart::ChartRenderData::Kde { data: kde_data },
                ChartKind::Heatmap => {
                    widgets::chart::ChartRenderData::Heatmap { data: heatmap_data }
                }
            };

            widgets::chart::render_chart_view(
                chart_area,
                buf,
                &mut self.chart_modal,
                &self.theme,
                render_data,
            );

            if self.chart_export_modal.active {
                let border = self.color("modal_border");
                let active = self.color("modal_border_active");
                // 4 rows (format, title, path, buttons) of 3 lines each + 2 for outer border = 14
                const CHART_EXPORT_MODAL_HEIGHT: u16 = 14;
                let modal_width = (chart_area.width * 3 / 4).clamp(40, 54);
                let modal_height = CHART_EXPORT_MODAL_HEIGHT
                    .min(chart_area.height)
                    .max(CHART_EXPORT_MODAL_HEIGHT);
                let modal_x = chart_area.x + chart_area.width.saturating_sub(modal_width) / 2;
                let modal_y = chart_area.y + chart_area.height.saturating_sub(modal_height) / 2;
                let modal_area = Rect {
                    x: modal_x,
                    y: modal_y,
                    width: modal_width,
                    height: modal_height,
                };
                widgets::chart_export_modal::render_chart_export_modal(
                    modal_area,
                    buf,
                    &mut self.chart_export_modal,
                    border,
                    active,
                );
            }
        }

        // Render loading progress popover (min 25 chars wide, max 25% of area; throbber spins via busy in controls)
        if matches!(self.loading_state, LoadingState::Loading { .. }) {
            let popover_rect = centered_rect_loading(area);
            App::render_loading_gauge(&self.loading_state, popover_rect, buf, &self.theme);
        }
        // Render export progress bar (overlay when exporting)
        if matches!(self.loading_state, LoadingState::Exporting { .. }) {
            App::render_loading_gauge(&self.loading_state, area, buf, &self.theme);
        }

        // Render confirmation modal (highest priority)
        if self.confirmation_modal.active {
            let popup_area = centered_rect_with_min(area, 64, 26, 50, 12);
            Clear.render(popup_area, buf);

            // Set background color for the modal
            let bg_color = self.color("background");
            Block::default()
                .style(Style::default().bg(bg_color))
                .render(popup_area, buf);

            let block = Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .title("Confirm")
                .border_style(Style::default().fg(self.color("modal_border_active")))
                .style(Style::default().bg(bg_color));
            let inner_area = block.inner(popup_area);
            block.render(popup_area, buf);

            // Split inner area into message and buttons
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Min(6),    // Message (minimum 6 lines for file path + question)
                    Constraint::Length(3), // Buttons
                ])
                .split(inner_area);

            // Render confirmation message (wrapped)
            Paragraph::new(self.confirmation_modal.message.as_str())
                .style(Style::default().fg(self.color("text_primary")).bg(bg_color))
                .wrap(ratatui::widgets::Wrap { trim: true })
                .render(chunks[0], buf);

            // Render Yes/No buttons
            let button_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Fill(1),
                    Constraint::Length(12), // Yes button
                    Constraint::Length(2),  // Spacing
                    Constraint::Length(12), // No button
                    Constraint::Fill(1),
                ])
                .split(chunks[1]);

            let yes_style = if self.confirmation_modal.focus_yes {
                Style::default().fg(self.color("modal_border_active"))
            } else {
                Style::default()
            };
            let no_style = if !self.confirmation_modal.focus_yes {
                Style::default().fg(self.color("modal_border_active"))
            } else {
                Style::default()
            };

            Paragraph::new("Yes")
                .centered()
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .border_style(yes_style),
                )
                .render(button_chunks[1], buf);

            Paragraph::new("No")
                .centered()
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .border_style(no_style),
                )
                .render(button_chunks[3], buf);
        }

        // Render success modal
        if self.success_modal.active {
            let popup_area = centered_rect(area, 70, 40);
            Clear.render(popup_area, buf);
            let block = Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .title("Success");
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

            // Render success message (wrapped)
            Paragraph::new(self.success_modal.message.as_str())
                .style(Style::default().fg(self.color("text_primary")))
                .wrap(ratatui::widgets::Wrap { trim: true })
                .render(chunks[0], buf);

            // Render OK button
            let ok_style = Style::default().fg(self.color("modal_border_active"));
            Paragraph::new("OK")
                .centered()
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .border_style(ok_style),
                )
                .render(chunks[1], buf);
        }

        // Render error modal
        if self.error_modal.active {
            let popup_area = centered_rect(area, 70, 40);
            Clear.render(popup_area, buf);
            let block = Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
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
            Paragraph::new("OK")
                .centered()
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .border_style(ok_style),
                )
                .render(chunks[1], buf);
        }

        if self.show_help
            || (self.template_modal.active && self.template_modal.show_help)
            || (self.analysis_modal.active && self.analysis_modal.show_help)
        {
            let popup_area = centered_rect(area, 80, 80);
            Clear.render(popup_area, buf);
            let (title, text): (String, String) = if self.analysis_modal.active
                && self.analysis_modal.show_help
            {
                match self.analysis_modal.view {
                    analysis_modal::AnalysisView::DistributionDetail => (
                        "Distribution Detail Help".to_string(),
                        help_strings::analysis_distribution_detail().to_string(),
                    ),
                    analysis_modal::AnalysisView::CorrelationDetail => (
                        "Correlation Detail Help".to_string(),
                        help_strings::analysis_correlation_detail().to_string(),
                    ),
                    analysis_modal::AnalysisView::Main => match self.analysis_modal.selected_tool {
                        Some(analysis_modal::AnalysisTool::DistributionAnalysis) => (
                            "Distribution Analysis Help".to_string(),
                            help_strings::analysis_distribution().to_string(),
                        ),
                        Some(analysis_modal::AnalysisTool::Describe) => (
                            "Describe Tool Help".to_string(),
                            help_strings::analysis_describe().to_string(),
                        ),
                        Some(analysis_modal::AnalysisTool::CorrelationMatrix) => (
                            "Correlation Matrix Help".to_string(),
                            help_strings::analysis_correlation_matrix().to_string(),
                        ),
                        None => (
                            "Analysis Help".to_string(),
                            "Select an analysis tool from the sidebar.".to_string(),
                        ),
                    },
                }
            } else if self.template_modal.active {
                (
                    "Template Help".to_string(),
                    help_strings::template().to_string(),
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
            let block = Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded);
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
                    // Split long lines into wrapped segments (at char boundaries so UTF-8 is safe)
                    let mut remaining = *line;
                    while !remaining.is_empty() {
                        let mut take = remaining.len().min(available_width);
                        while take > 0 && !remaining.is_char_boundary(take) {
                            take -= 1;
                        }
                        // If take is 0 (e.g. first char is multi-byte and width is 1), advance by one char
                        let take_len = if take == 0 {
                            remaining.chars().next().map_or(0, |c| c.len_utf8())
                        } else {
                            take
                        };
                        let (chunk, rest) = remaining.split_at(take_len);
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
        // Dim controls when any modal is active (except analysis/chart modals use their own controls)
        let is_modal_active = self.show_help
            || self.input_mode == InputMode::Editing
            || self.input_mode == InputMode::SortFilter
            || self.input_mode == InputMode::PivotMelt
            || self.input_mode == InputMode::Info
            || self.sort_filter_modal.active;

        // Build controls - use analysis-specific controls if analysis modal is active
        let use_unicode_throbber = std::env::var("LANG")
            .map(|l| l.to_uppercase().contains("UTF-8"))
            .unwrap_or(false);
        let mut controls = Controls::with_row_count(row_count.unwrap_or(0))
            .with_colors(
                self.color("controls_bg"),
                self.color("keybind_hints"),
                self.color("keybind_labels"),
                self.color("throbber"),
            )
            .with_unicode_throbber(use_unicode_throbber);

        if self.analysis_modal.active {
            // Build analysis-specific controls based on view
            let mut analysis_controls = vec![
                ("Esc", "Back"),
                ("↑↓", "Navigate"),
                ("←→", "Scroll Columns"),
                ("Tab", "Sidebar"),
                ("Enter", "Select"),
            ];

            // Show r Resample only when sampling is enabled and data was sampled
            if self.sampling_threshold.is_some() {
                if let Some(results) = &self.analysis_modal.analysis_results {
                    if results.sample_size.is_some() {
                        analysis_controls.push(("r", "Resample"));
                    }
                }
            }

            controls = controls.with_custom_controls(analysis_controls);
        } else if self.input_mode == InputMode::Chart {
            let chart_controls = vec![("Esc", "Back"), ("e", "Export")];
            controls = controls.with_custom_controls(chart_controls);
        } else {
            controls = controls
                .with_dimmed(is_modal_active)
                .with_query_active(query_active);
        }

        if self.busy {
            self.throbber_frame = self.throbber_frame.wrapping_add(1);
        }
        controls = controls.with_busy(self.busy, self.throbber_frame);
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

/// Like `centered_rect` but enforces minimum width and height so the dialog
/// stays usable on very small terminals.
fn centered_rect_with_min(
    r: Rect,
    percent_x: u16,
    percent_y: u16,
    min_width: u16,
    min_height: u16,
) -> Rect {
    let inner = centered_rect(r, percent_x, percent_y);
    let width = inner.width.max(min_width).min(r.width);
    let height = inner.height.max(min_height).min(r.height);
    let x = r.x + r.width.saturating_sub(width) / 2;
    let y = r.y + r.height.saturating_sub(height) / 2;
    Rect::new(x, y, width, height)
}

/// Rect for the loading progress popover: at least 25 characters wide, at most 25% of area width.
/// Height is at least 5 lines, at most 20% of area height.
fn centered_rect_loading(r: Rect) -> Rect {
    const MIN_WIDTH: u16 = 25;
    const MAX_WIDTH_PERCENT: u16 = 25;
    const MIN_HEIGHT: u16 = 5;
    const MAX_HEIGHT_PERCENT: u16 = 20;

    let width = (r.width * MAX_WIDTH_PERCENT / 100)
        .max(MIN_WIDTH)
        .min(r.width);
    let height = (r.height * MAX_HEIGHT_PERCENT / 100)
        .max(MIN_HEIGHT)
        .min(r.height);

    let x = r.x + r.width.saturating_sub(width) / 2;
    let y = r.y + r.height.saturating_sub(height) / 2;
    Rect::new(x, y, width, height)
}

/// Run the TUI with either file paths or an existing LazyFrame. Single event loop used by CLI and Python binding.
pub fn run(input: RunInput, config: Option<AppConfig>, debug: bool) -> Result<()> {
    use std::io::Write;
    use std::sync::{mpsc, Mutex, Once};

    let config = config.unwrap_or_else(|| {
        AppConfig::load(APP_NAME).unwrap_or_else(|e| {
            eprintln!("Warning: Failed to load config: {}. Using defaults.", e);
            AppConfig::default()
        })
    });

    let theme = Theme::from_config(&config.theme).unwrap_or_else(|e| {
        eprintln!(
            "Warning: Failed to create theme from config: {}. Using default theme.",
            e
        );
        Theme::from_config(&AppConfig::default().theme)
            .expect("Default theme should always be valid")
    });

    // Install color_eyre at most once per process (e.g. first datui.view() in Python).
    // Subsequent run() calls skip install and reuse the result; no error-message detection.
    static COLOR_EYRE_INIT: Once = Once::new();
    static INSTALL_RESULT: Mutex<Option<Result<(), color_eyre::Report>>> = Mutex::new(None);
    COLOR_EYRE_INIT.call_once(|| {
        *INSTALL_RESULT.lock().expect("color_eyre install mutex") = Some(color_eyre::install());
    });
    if let Some(Err(e)) = INSTALL_RESULT
        .lock()
        .expect("color_eyre install mutex")
        .as_ref()
    {
        return Err(color_eyre::eyre::eyre!(e.to_string()));
    }
    // Validate every local path before initing the terminal so the Python binding can raise
    // FileNotFoundError (no TTY required). Otherwise multi-path load failures become
    // AppEvent::Crash(msg) and return Err(eyre!(msg)) with no io::Error in the chain → RuntimeError.
    if let RunInput::Paths(ref paths, _) = input {
        for path in paths {
            let s = path.to_string_lossy();
            let is_remote = s.starts_with("s3://")
                || s.starts_with("gs://")
                || s.starts_with("http://")
                || s.starts_with("https://");
            if !is_remote && !path.exists() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("File not found: {}", path.display()),
                )
                .into());
            }
        }
    }
    let mut terminal = ratatui::try_init().map_err(|e| {
        color_eyre::eyre::eyre!(
            "datui requires an interactive terminal (TTY). No terminal detected: {}. \
             Run from a terminal or ensure stdout is connected to a TTY.",
            e
        )
    })?;
    let (tx, rx) = mpsc::channel::<AppEvent>();
    let mut app = App::new_with_config(tx.clone(), theme, config.clone());
    if debug {
        app.enable_debug();
    }

    terminal.draw(|frame| frame.render_widget(&mut app, frame.area()))?;

    match input {
        RunInput::Paths(paths, opts) => {
            tx.send(AppEvent::Open(paths, opts))?;
        }
        RunInput::LazyFrame(lf, opts) => {
            // Show loading dialog immediately so it is visible when launch is from Python/LazyFrame
            // (before sending the event and before any blocking work in the event handler).
            app.set_loading_phase("Scanning input", 10);
            terminal.draw(|frame| frame.render_widget(&mut app, frame.area()))?;
            let _ = std::io::stdout().flush();
            // Brief pause so the terminal can display the frame when run from Python (e.g. maturin).
            std::thread::sleep(std::time::Duration::from_millis(150));
            tx.send(AppEvent::OpenLazyFrame(lf, opts))?;
        }
    }

    // Process load events and draw so the loading progress dialog updates (e.g. "Caching schema")
    // before any blocking work. Keeps processing until no event is received (timeout).
    loop {
        let event = match rx.recv_timeout(std::time::Duration::from_millis(50)) {
            Ok(ev) => ev,
            Err(mpsc::RecvTimeoutError::Timeout) => break,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        match event {
            AppEvent::Exit => break,
            AppEvent::Crash(msg) => {
                ratatui::restore();
                return Err(color_eyre::eyre::eyre!(msg));
            }
            ev => {
                if let Some(next) = app.event(&ev) {
                    let _ = tx.send(next);
                }
                terminal.draw(|frame| frame.render_widget(&mut app, frame.area()))?;
                let _ = std::io::stdout().flush();
                // After processing DoLoadSchema we've drawn "Caching schema"; next event is DoLoadSchemaBlocking (blocking).
                // Leave it for the main loop so we don't block here.
                if matches!(ev, AppEvent::DoLoadSchema(..)) {
                    break;
                }
            }
        }
    }

    loop {
        if crossterm::event::poll(std::time::Duration::from_millis(
            config.performance.event_poll_interval_ms,
        ))? {
            match crossterm::event::read()? {
                crossterm::event::Event::Key(key) => tx.send(AppEvent::Key(key))?,
                crossterm::event::Event::Resize(cols, rows) => {
                    tx.send(AppEvent::Resize(cols, rows))?
                }
                _ => {}
            }
        }

        let updated = match rx.recv_timeout(std::time::Duration::from_millis(0)) {
            Ok(event) => {
                match event {
                    AppEvent::Exit => break,
                    AppEvent::Crash(msg) => {
                        ratatui::restore();
                        return Err(color_eyre::eyre::eyre!(msg));
                    }
                    event => {
                        if let Some(next) = app.event(&event) {
                            tx.send(next)?;
                        }
                    }
                }
                true
            }
            Err(mpsc::RecvTimeoutError::Timeout) => false,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };

        if updated {
            terminal.draw(|frame| frame.render_widget(&mut app, frame.area()))?;
            if app.should_drain_keys() {
                while crossterm::event::poll(std::time::Duration::from_millis(0))? {
                    let _ = crossterm::event::read();
                }
                app.clear_drain_keys_request();
            }
        }
    }

    ratatui::restore();
    Ok(())
}
