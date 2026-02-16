use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use polars::datatypes::AnyValue;
use polars::datatypes::DataType;
#[cfg(feature = "cloud")]
use polars::io::cloud::{AmazonS3ConfigKey, CloudOptions};
use polars::prelude::{col, len, DataFrame, LazyFrame, Schema};
#[cfg(feature = "cloud")]
use polars::prelude::{PlRefPath, ScanArgsParquet};
use std::path::{Path, PathBuf};
use std::sync::{mpsc::Sender, Arc};
use widgets::info::{read_parquet_metadata, InfoFocus, InfoModal, InfoTab, ParquetMetadataCache};

use ratatui::style::{Color, Style};
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};

use ratatui::widgets::{Block, Clear};

pub mod analysis_modal;
pub mod cache;
pub mod chart_data;
pub mod chart_export;
pub mod chart_export_modal;
pub mod chart_modal;
pub mod cli;
#[cfg(feature = "cloud")]
mod cloud_hive;
pub mod config;
pub mod error_display;
pub mod export_modal;
pub mod filter_modal;
pub(crate) mod help_strings;
pub mod pivot_melt_modal;
mod query;
mod render;
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
use widgets::datatable::DataTableState;
use widgets::debug::DebugState;
use widgets::template_modal::{CreateFocus, TemplateFocus, TemplateModal, TemplateModalMode};
use widgets::text_input::{TextInput, TextInputEvent};

/// Application name used for cache directory and other app-specific paths
pub const APP_NAME: &str = "datui";

/// Re-export compression format and file format from CLI module
pub use cli::{CompressionFormat, FileFormat};

/// Map FileFormat to ExportFormat for default export. Tsv/Psv map to Csv; Orc/Excel have no export variant.
fn file_format_to_export_format(f: FileFormat) -> Option<ExportFormat> {
    match f {
        FileFormat::Parquet => Some(ExportFormat::Parquet),
        FileFormat::Csv | FileFormat::Tsv | FileFormat::Psv => Some(ExportFormat::Csv),
        FileFormat::Json => Some(ExportFormat::Json),
        FileFormat::Jsonl => Some(ExportFormat::Ndjson),
        FileFormat::Arrow => Some(ExportFormat::Ipc),
        FileFormat::Avro => Some(ExportFormat::Avro),
        FileFormat::Orc | FileFormat::Excel => None,
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
                "infer_schema_length_data.csv",
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

    /// Only one query type is returned; SQL overrides fuzzy over DSL. Used when saving templates.
    #[test]
    fn test_active_query_settings_only_one_set() {
        use super::active_query_settings;

        let (q, sql, fuzzy) = active_query_settings("", "", "");
        assert!(q.is_none() && sql.is_none() && fuzzy.is_none());

        let (q, sql, fuzzy) = active_query_settings("select a", "SELECT 1", "foo");
        assert!(q.is_none() && sql.as_deref() == Some("SELECT 1") && fuzzy.is_none());

        let (q, sql, fuzzy) = active_query_settings("select a", "", "foo bar");
        assert!(q.is_none() && sql.is_none() && fuzzy.as_deref() == Some("foo bar"));

        let (q, sql, fuzzy) = active_query_settings("  select a  ", "", "");
        assert!(q.as_deref() == Some("select a") && sql.is_none() && fuzzy.is_none());
    }
}

/// Which CSV string columns to trim and parse (date/datetime/time/duration/int/float). Default: all. None = disabled (e.g. --no-parse-strings).
#[derive(Clone, Debug)]
pub enum ParseStringsTarget {
    /// Apply to all string columns.
    All,
    /// Apply only to these columns (must exist and be string type).
    Columns(Vec<String>),
}

#[derive(Clone)]
pub struct OpenOptions {
    pub delimiter: Option<u8>,
    pub has_header: Option<bool>,
    pub skip_lines: Option<usize>,
    pub skip_rows: Option<usize>,
    /// Skip this many rows at the end of the file (e.g. vendor footer or trailing garbage). Applied after load for CSV.
    pub skip_tail_rows: Option<usize>,
    pub compression: Option<CompressionFormat>,
    /// When set, bypass extension-based format detection and use this format (e.g. for URLs or temp files without extension).
    pub format: Option<FileFormat>,
    pub pages_lookahead: Option<usize>,
    pub pages_lookback: Option<usize>,
    pub max_buffered_rows: Option<usize>,
    pub max_buffered_mb: Option<usize>,
    pub row_numbers: bool,
    pub row_start_index: usize,
    /// When true, use hive load path for directory/glob; single file uses normal load.
    pub hive: bool,
    /// When true (default), infer Hive/partitioned Parquet schema from one file for faster "Caching schema". When false, use Polars collect_schema().
    pub single_spine_schema: bool,
    /// When true, CSV reader tries to parse string columns as dates (e.g. YYYY-MM-DD, ISO datetime).
    pub parse_dates: bool,
    /// When set, trim and parse CSV string columns: None = off, Some(true) = all columns, Some(cols) = those columns only.
    pub parse_strings: Option<ParseStringsTarget>,
    /// Sample size (rows) for inferring types when parse_strings is enabled; single file or multiple/partitioned.
    pub parse_strings_sample_rows: usize,
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
    /// When true, use Polars streaming engine for LazyFrame collect when the streaming feature is enabled.
    pub polars_streaming: bool,
    /// When true, cast Date/Datetime pivot index columns to Int32 before pivot to avoid Polars 0.52 panic.
    pub workaround_pivot_date_index: bool,
    /// Null value specs for CSV: global strings and/or "COL=VAL" for per-column. Empty = use Polars default.
    pub null_values: Option<Vec<String>>,
    /// Number of rows to use when inferring CSV schema. None = Polars default (100). Larger values reduce risk of inferring wrong type (e.g. int then N/A).
    pub infer_schema_length: Option<usize>,
    /// When true, CSV reader ignores parse errors and continues with the next batch.
    pub ignore_errors: bool,
    /// When true, show the debug overlay (session info, performance, query, etc.).
    pub debug: bool,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self {
            delimiter: None,
            has_header: None,
            skip_lines: None,
            skip_rows: None,
            skip_tail_rows: None,
            compression: None,
            format: None,
            pages_lookahead: None,
            pages_lookback: None,
            max_buffered_rows: None,
            max_buffered_mb: None,
            row_numbers: false,
            row_start_index: 1,
            hive: false,
            single_spine_schema: true,
            parse_dates: true,
            parse_strings: None,
            parse_strings_sample_rows: 1000,
            decompress_in_memory: false,
            temp_dir: None,
            excel_sheet: None,
            s3_endpoint_url_override: None,
            s3_access_key_id_override: None,
            s3_secret_access_key_override: None,
            s3_region_override: None,
            polars_streaming: true,
            workaround_pivot_date_index: true,
            null_values: None,
            infer_schema_length: None,
            ignore_errors: false,
            debug: false,
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

    pub fn with_workaround_pivot_date_index(mut self, workaround_pivot_date_index: bool) -> Self {
        self.workaround_pivot_date_index = workaround_pivot_date_index;
        self
    }

    /// When loading CSV: use Polars try_parse_dates only if parse_strings is not set.
    /// When parse_strings is set we do our own date parsing (with strict: false), so we disable
    /// Polars' try_parse_dates to avoid "could not find an appropriate format" errors.
    pub fn csv_try_parse_dates(&self) -> bool {
        self.parse_strings.is_none() && self.parse_dates
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
        opts.skip_tail_rows = args.skip_tail_rows.or(config.file_loading.skip_tail_rows);

        // Handle has_header: CLI no_header flag overrides config
        opts.has_header = if let Some(no_header) = args.no_header {
            Some(!no_header)
        } else {
            config.file_loading.has_header
        };

        // Compression: CLI only (auto-detect from extension when not specified)
        opts.compression = args.compression;

        // Format: CLI only (auto-detect from extension when not specified)
        opts.format = args.format;

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

        // Single-spine schema: CLI overrides config; default true
        opts.single_spine_schema = args
            .single_spine_schema
            .or(config.file_loading.single_spine_schema)
            .unwrap_or(true);

        // CSV date inference: CLI overrides config; default true
        opts.parse_dates = args
            .parse_dates
            .or(config.file_loading.parse_dates)
            .unwrap_or(true);

        // Parse strings (trim + type inference). Default: all CSV string columns. --no-parse-strings disables; --parse-strings=COL limits to columns.
        if args.no_parse_strings {
            opts.parse_strings = None;
        } else if !args.parse_strings.is_empty() {
            let has_all = args.parse_strings.iter().any(|s| s.is_empty());
            opts.parse_strings = Some(if has_all {
                ParseStringsTarget::All
            } else {
                let cols: Vec<String> = args
                    .parse_strings
                    .iter()
                    .filter(|s| !s.is_empty())
                    .cloned()
                    .collect::<std::collections::HashSet<_>>()
                    .into_iter()
                    .collect();
                ParseStringsTarget::Columns(cols)
            });
        } else if config.file_loading.parse_strings == Some(false) {
            opts.parse_strings = None;
        } else {
            opts.parse_strings = Some(ParseStringsTarget::All);
        }
        opts.parse_strings_sample_rows = config
            .file_loading
            .parse_strings_sample_rows
            .unwrap_or(1000);

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

        opts.polars_streaming = config.performance.polars_streaming;

        opts.workaround_pivot_date_index = args.workaround_pivot_date_index.unwrap_or(true);

        // Debug: CLI flag overrides config
        opts.debug = args.debug || config.debug.enabled;

        // Null values: merge config list with CLI list (CLI appended); if either is non-empty, set
        let config_nulls = config.file_loading.null_values.as_deref().unwrap_or(&[]);
        let cli_nulls = &args.null_value;
        if config_nulls.is_empty() && cli_nulls.is_empty() {
            opts.null_values = None;
        } else {
            opts.null_values = Some(
                config_nulls
                    .iter()
                    .chain(cli_nulls.iter())
                    .cloned()
                    .collect(),
            );
        }

        // CSV schema inference: CLI overrides config; default 1000 (Polars default is 100)
        opts.infer_schema_length = args
            .infer_schema_length
            .or(config.file_loading.infer_schema_length)
            .or(Some(1000));

        // CSV ignore parse errors: CLI overrides config; default false
        opts.ignore_errors = args
            .ignore_errors
            .or(config.file_loading.ignore_errors)
            .unwrap_or(false);

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
    /// Build LazyFrame for CSV with --parse-strings (phase already set to "Scanning string columns" so UI shows it).
    DoLoadCsvWithParseStrings(Vec<PathBuf>, OpenOptions),
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
    SqlSearch(String),
    FuzzySearch(String),
    Filter(Vec<FilterStatement>),
    Sort(Vec<String>, bool),         // Columns, Ascending
    ColumnOrder(Vec<String>, usize), // Column order, locked columns count
    Pivot(PivotSpec),
    Melt(MeltSpec),
    Export(PathBuf, ExportFormat, ExportOptions), // Path, format, options
    ChartExport(PathBuf, ChartExportFormat, String, u32, u32), // path, format, title, width, height
    DoChartExport(PathBuf, ChartExportFormat, String, u32, u32), // Deferred: run chart export
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

/// Query dialog tab: SQL-Like (current parser), Fuzzy, or SQL (future).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryTab {
    #[default]
    SqlLike,
    Fuzzy,
    Sql,
}

impl QueryTab {
    fn next(self) -> Self {
        match self {
            QueryTab::SqlLike => QueryTab::Fuzzy,
            QueryTab::Fuzzy => QueryTab::Sql,
            QueryTab::Sql => QueryTab::SqlLike,
        }
    }
    fn prev(self) -> Self {
        match self {
            QueryTab::SqlLike => QueryTab::Sql,
            QueryTab::Fuzzy => QueryTab::SqlLike,
            QueryTab::Sql => QueryTab::Fuzzy,
        }
    }
    fn index(self) -> usize {
        match self {
            QueryTab::SqlLike => 0,
            QueryTab::Fuzzy => 1,
            QueryTab::Sql => 2,
        }
    }
}

/// Focus within the query dialog: tab bar or input (SQL-Like only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryFocus {
    TabBar,
    #[default]
    Input,
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

/// Pending remote download; shown in confirmation modal before starting download.
#[cfg(any(feature = "http", feature = "cloud"))]
#[derive(Clone)]
pub enum PendingDownload {
    #[cfg(feature = "http")]
    Http {
        url: String,
        size: Option<u64>,
        options: OpenOptions,
    },
    #[cfg(feature = "cloud")]
    S3 {
        url: String,
        size: Option<u64>,
        options: OpenOptions,
    },
    #[cfg(feature = "cloud")]
    Gcs {
        url: String,
        size: Option<u64>,
        options: OpenOptions,
    },
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

/// At most one query type can be active. Returns (query, sql_query, fuzzy_query) with only the
/// active one set (SQL takes precedence over fuzzy over DSL query). Used when saving template settings.
fn active_query_settings(
    dsl_query: &str,
    sql_query: &str,
    fuzzy_query: &str,
) -> (Option<String>, Option<String>, Option<String>) {
    let sql_trimmed = sql_query.trim();
    let fuzzy_trimmed = fuzzy_query.trim();
    let dsl_trimmed = dsl_query.trim();
    if !sql_trimmed.is_empty() {
        (None, Some(sql_trimmed.to_string()), None)
    } else if !fuzzy_trimmed.is_empty() {
        (None, None, Some(fuzzy_trimmed.to_string()))
    } else if !dsl_trimmed.is_empty() {
        (Some(dsl_trimmed.to_string()), None, None)
    } else {
        (None, None, None)
    }
}

// Helper struct to save state before template application
struct TemplateApplicationState {
    lf: LazyFrame,
    schema: Arc<Schema>,
    active_query: String,
    active_sql_query: String,
    active_fuzzy_query: String,
    filters: Vec<FilterStatement>,
    sort_columns: Vec<String>,
    sort_ascending: bool,
    column_order: Vec<String>,
    locked_columns_count: usize,
}

#[derive(Default)]
pub(crate) struct ChartCache {
    pub(crate) xy: Option<ChartCacheXY>,
    pub(crate) x_range: Option<ChartCacheXRange>,
    pub(crate) histogram: Option<ChartCacheHistogram>,
    pub(crate) box_plot: Option<ChartCacheBoxPlot>,
    pub(crate) kde: Option<ChartCacheKde>,
    pub(crate) heatmap: Option<ChartCacheHeatmap>,
}

impl ChartCache {
    fn clear(&mut self) {
        *self = Self::default();
    }
}

pub(crate) struct ChartCacheXY {
    pub(crate) x_column: String,
    pub(crate) y_columns: Vec<String>,
    pub(crate) row_limit: Option<usize>,
    pub(crate) series: Vec<Vec<(f64, f64)>>,
    pub(crate) series_log: Option<Vec<Vec<(f64, f64)>>>,
    pub(crate) x_axis_kind: chart_data::XAxisTemporalKind,
}

pub(crate) struct ChartCacheXRange {
    pub(crate) x_column: String,
    pub(crate) row_limit: Option<usize>,
    pub(crate) x_min: f64,
    pub(crate) x_max: f64,
    pub(crate) x_axis_kind: chart_data::XAxisTemporalKind,
}

pub(crate) struct ChartCacheHistogram {
    pub(crate) column: String,
    pub(crate) bins: usize,
    pub(crate) row_limit: Option<usize>,
    pub(crate) data: chart_data::HistogramData,
}

pub(crate) struct ChartCacheBoxPlot {
    pub(crate) column: String,
    pub(crate) row_limit: Option<usize>,
    pub(crate) data: chart_data::BoxPlotData,
}

pub(crate) struct ChartCacheKde {
    pub(crate) column: String,
    pub(crate) bandwidth_factor: f64,
    pub(crate) row_limit: Option<usize>,
    pub(crate) data: chart_data::KdeData,
}

pub(crate) struct ChartCacheHeatmap {
    pub(crate) x_column: String,
    pub(crate) y_column: String,
    pub(crate) bins: usize,
    pub(crate) row_limit: Option<usize>,
    pub(crate) data: chart_data::HeatmapData,
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
    sql_input: TextInput,   // SQL tab input with its own history (id "sql")
    fuzzy_input: TextInput, // Fuzzy tab input with its own history (id "fuzzy")
    pub input_mode: InputMode,
    input_type: Option<InputType>,
    query_tab: QueryTab,
    query_focus: QueryFocus,
    pub sort_filter_modal: SortFilterModal,
    pub pivot_melt_modal: PivotMeltModal,
    pub template_modal: TemplateModal,
    pub analysis_modal: AnalysisModal,
    pub chart_modal: ChartModal,
    pub chart_export_modal: ChartExportModal,
    pub export_modal: ExportModal,
    pub(crate) chart_cache: ChartCache,
    error_modal: ErrorModal,
    success_modal: SuccessModal,
    confirmation_modal: ConfirmationModal,
    pending_export: Option<(PathBuf, ExportFormat, ExportOptions)>, // Store export request while waiting for confirmation
    /// Collected DataFrame between DoExportCollect and DoExportWrite (two-phase export progress).
    export_df: Option<DataFrame>,
    pending_chart_export: Option<(PathBuf, ChartExportFormat, String, u32, u32)>,
    /// Pending remote file download (HTTP/S3/GCS) while waiting for user confirmation. Size is from HEAD when available.
    #[cfg(any(feature = "http", feature = "cloud"))]
    pending_download: Option<PendingDownload>,
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
                TemplateManager::new(&last_resort)
                    .unwrap_or_else(|_| TemplateManager::empty(&last_resort))
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
            sql_input: TextInput::new()
                .with_history_limit(app_config.query.history_limit)
                .with_theme(&theme)
                .with_history("sql".to_string()),
            fuzzy_input: TextInput::new()
                .with_history_limit(app_config.query.history_limit)
                .with_theme(&theme)
                .with_history("fuzzy".to_string()),
            input_mode: InputMode::Normal,
            input_type: None,
            query_tab: QueryTab::SqlLike,
            query_focus: QueryFocus::Input,
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
            #[cfg(any(feature = "http", feature = "cloud"))]
            pending_download: None,
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
        let is_csv = options.format == Some(FileFormat::Csv)
            || path
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
            // Phase: Reading data or Scanning string columns (decompressing + parsing CSV; user may see "Decompressing" until we return)
            if let LoadingState::Loading {
                file_path,
                file_size,
                ..
            } = &self.loading_state
            {
                self.loading_state = LoadingState::Loading {
                    file_path: file_path.clone(),
                    file_size: *file_size,
                    current_phase: if options.parse_strings.is_some() {
                        "Scanning string columns".to_string()
                    } else {
                        "Reading data".to_string()
                    },
                    progress_percent: if options.parse_strings.is_some() {
                        55
                    } else {
                        50
                    },
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
        // Phase 2: Building lazyframe (or Scanning string columns for CSV when --parse-strings)
        let effective_format = options.format.or_else(|| FileFormat::from_path(path));
        let csv_parse_strings =
            effective_format == Some(FileFormat::Csv) && options.parse_strings.is_some();
        if let LoadingState::Loading {
            file_path,
            file_size,
            ..
        } = &self.loading_state
        {
            self.loading_state = LoadingState::Loading {
                file_path: file_path.clone(),
                file_size: *file_size,
                current_phase: if csv_parse_strings {
                    "Scanning string columns".to_string()
                } else {
                    "Building lazyframe".to_string()
                },
                progress_percent: if csv_parse_strings { 55 } else { 60 },
            };
        }

        // Determine and store original file format (from explicit format or first path)
        let original_format = effective_format
            .and_then(file_format_to_export_format)
            .or_else(|| {
                path.extension().and_then(|e| e.to_str()).and_then(|ext| {
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
                })
            });

        let lf = if paths.len() > 1 {
            // Multiple files: same format assumed (from first path or --format), concatenated into one LazyFrame
            match effective_format {
                Some(FileFormat::Parquet) => DataTableState::from_parquet_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Csv) => DataTableState::from_csv_paths(paths, options)?,
                Some(FileFormat::Json) => DataTableState::from_json_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Jsonl) => DataTableState::from_json_lines_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Arrow) => DataTableState::from_ipc_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Avro) => DataTableState::from_avro_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Orc) => DataTableState::from_orc_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Tsv) | Some(FileFormat::Psv) | Some(FileFormat::Excel) | None => {
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
            match effective_format {
                Some(FileFormat::Parquet) => DataTableState::from_parquet(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Csv) => DataTableState::from_csv(path, options)?,
                Some(FileFormat::Tsv) => DataTableState::from_delimited(
                    path,
                    b'\t',
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Psv) => DataTableState::from_delimited(
                    path,
                    b'|',
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Json) => DataTableState::from_json(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Jsonl) => DataTableState::from_json_lines(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Arrow) => DataTableState::from_ipc(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Avro) => DataTableState::from_avro(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Excel) => DataTableState::from_excel(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                    options.excel_sheet.as_deref(),
                )?,
                Some(FileFormat::Orc) => DataTableState::from_orc(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                None => {
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
        // Store delimiter based on file type (use effective format when set)
        self.original_file_delimiter = match effective_format {
            Some(FileFormat::Csv) => Some(options.delimiter.unwrap_or(b',')),
            Some(FileFormat::Tsv) => Some(b'\t'),
            Some(FileFormat::Psv) => Some(b'|'),
            _ => path.extension().and_then(|e| e.to_str()).and_then(|ext| {
                if ext.eq_ignore_ascii_case("csv") {
                    Some(options.delimiter.unwrap_or(b','))
                } else if ext.eq_ignore_ascii_case("tsv") {
                    Some(b'\t')
                } else if ext.eq_ignore_ascii_case("psv") {
                    Some(b'|')
                } else {
                    None
                }
            }),
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

    #[cfg(feature = "cloud")]
    fn build_s3_object_store(
        s3_url: &str,
        cloud: &crate::config::CloudConfig,
        options: &OpenOptions,
    ) -> Result<Arc<dyn object_store::ObjectStore>> {
        let (path_part, _ext) = source::url_path_extension(s3_url);
        let (bucket, _key) = path_part
            .split_once('/')
            .ok_or_else(|| color_eyre::eyre::eyre!("S3 URL must be s3://bucket/key"))?;
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
        Ok(Arc::new(store))
    }

    #[cfg(feature = "cloud")]
    fn build_gcs_object_store(gs_url: &str) -> Result<Arc<dyn object_store::ObjectStore>> {
        let (path_part, _ext) = source::url_path_extension(gs_url);
        let (bucket, _key) = path_part
            .split_once('/')
            .ok_or_else(|| color_eyre::eyre::eyre!("GCS URL must be gs://bucket/key"))?;
        let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
            .with_url(gs_url)
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| color_eyre::eyre::eyre!("GCS config failed: {}", e))?;
        Ok(Arc::new(store))
    }

    /// Human-readable byte size for download confirmation modal.
    fn format_bytes(n: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;
        const TB: u64 = GB * 1024;
        if n >= TB {
            format!("{:.2} TB", n as f64 / TB as f64)
        } else if n >= GB {
            format!("{:.2} GB", n as f64 / GB as f64)
        } else if n >= MB {
            format!("{:.2} MB", n as f64 / MB as f64)
        } else if n >= KB {
            format!("{:.2} KB", n as f64 / KB as f64)
        } else {
            format!("{} bytes", n)
        }
    }

    #[cfg(feature = "http")]
    fn fetch_remote_size_http(url: &str) -> Result<Option<u64>> {
        let response = ureq::request("HEAD", url)
            .timeout(std::time::Duration::from_secs(15))
            .call();
        match response {
            Ok(r) => Ok(r
                .header("Content-Length")
                .and_then(|s| s.parse::<u64>().ok())),
            Err(_) => Ok(None),
        }
    }

    #[cfg(feature = "cloud")]
    fn fetch_remote_size_s3(
        s3_url: &str,
        cloud: &crate::config::CloudConfig,
        options: &OpenOptions,
    ) -> Result<Option<u64>> {
        use object_store::path::Path as OsPath;
        use object_store::ObjectStore;

        let (path_part, _ext) = source::url_path_extension(s3_url);
        let (bucket, key) = path_part
            .split_once('/')
            .ok_or_else(|| color_eyre::eyre::eyre!("S3 URL must be s3://bucket/key"))?;
        if key.is_empty() {
            return Ok(None);
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
        match rt.block_on(store.head(&path)) {
            Ok(meta) => Ok(Some(meta.size)),
            Err(_) => Ok(None),
        }
    }

    #[cfg(feature = "cloud")]
    fn fetch_remote_size_gcs(gs_url: &str, _options: &OpenOptions) -> Result<Option<u64>> {
        use object_store::path::Path as OsPath;
        use object_store::ObjectStore;

        let (path_part, _ext) = source::url_path_extension(gs_url);
        let (bucket, key) = path_part
            .split_once('/')
            .ok_or_else(|| color_eyre::eyre::eyre!("GCS URL must be gs://bucket/key"))?;
        if key.is_empty() {
            return Ok(None);
        }
        let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
            .with_url(gs_url)
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| color_eyre::eyre::eyre!("GCS config failed: {}", e))?;
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| color_eyre::eyre::eyre!("Could not start runtime: {}", e))?;
        let path = OsPath::from(key);
        match rt.block_on(store.head(&path)) {
            Ok(meta) => Ok(Some(meta.size)),
            Err(_) => Ok(None),
        }
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
                    let cloud_opts = Self::build_s3_cloud_options(&self.app_config.cloud, options);
                    let pl_path = PlRefPath::new(&full);
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
                    let pl_path = PlRefPath::new(&full);
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

        let effective_format = options.format.or_else(|| FileFormat::from_path(path));

        let lf = if paths.len() > 1 {
            match effective_format {
                Some(FileFormat::Parquet) => DataTableState::from_parquet_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Csv) => DataTableState::from_csv_paths(paths, options)?,
                Some(FileFormat::Json) => DataTableState::from_json_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Jsonl) => DataTableState::from_json_lines_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Arrow) => DataTableState::from_ipc_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Avro) => DataTableState::from_avro_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Orc) => DataTableState::from_orc_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Tsv) | Some(FileFormat::Psv) | Some(FileFormat::Excel) | None => {
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
            match effective_format {
                Some(FileFormat::Parquet) => DataTableState::from_parquet(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Csv) => DataTableState::from_csv(path, options)?,
                Some(FileFormat::Tsv) => DataTableState::from_delimited(
                    path,
                    b'\t',
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Psv) => DataTableState::from_delimited(
                    path,
                    b'|',
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Json) => DataTableState::from_json(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Jsonl) => DataTableState::from_json_lines(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Arrow) => DataTableState::from_ipc(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Avro) => DataTableState::from_avro(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Excel) => DataTableState::from_excel(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                    options.excel_sheet.as_deref(),
                )?,
                Some(FileFormat::Orc) => DataTableState::from_orc(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                None => {
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
                        if let Some((path, format, title, width, height)) =
                            self.pending_chart_export.take()
                        {
                            self.confirmation_modal.hide();
                            return Some(AppEvent::ChartExport(path, format, title, width, height));
                        }
                        if let Some((path, format, options)) = self.pending_export.take() {
                            self.confirmation_modal.hide();
                            return Some(AppEvent::Export(path, format, options));
                        }
                        #[cfg(any(feature = "http", feature = "cloud"))]
                        if let Some(pending) = self.pending_download.take() {
                            self.confirmation_modal.hide();
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
                            return Some(match pending {
                                #[cfg(feature = "http")]
                                PendingDownload::Http { url, options, .. } => {
                                    AppEvent::DoDownloadHttp(url, options)
                                }
                                #[cfg(feature = "cloud")]
                                PendingDownload::S3 { url, options, .. } => {
                                    AppEvent::DoDownloadS3ToTemp(url, options)
                                }
                                #[cfg(feature = "cloud")]
                                PendingDownload::Gcs { url, options, .. } => {
                                    AppEvent::DoDownloadGcsToTemp(url, options)
                                }
                            });
                        }
                    } else {
                        // User cancelled: if chart export overwrite, reopen chart export modal with path pre-filled
                        if let Some((path, format, _, _, _)) = self.pending_chart_export.take() {
                            self.chart_export_modal.reopen_with_path(&path, format);
                        }
                        self.pending_export = None;
                        #[cfg(any(feature = "http", feature = "cloud"))]
                        if self.pending_download.take().is_some() {
                            self.confirmation_modal.hide();
                            return Some(AppEvent::Exit);
                        }
                        self.confirmation_modal.hide();
                    }
                }
                KeyCode::Esc => {
                    // Cancel: if chart export overwrite, reopen chart export modal with path pre-filled
                    if let Some((path, format, _, _, _)) = self.pending_chart_export.take() {
                        self.chart_export_modal.reopen_with_path(&path, format);
                    }
                    self.pending_export = None;
                    #[cfg(any(feature = "http", feature = "cloud"))]
                    if self.pending_download.take().is_some() {
                        self.confirmation_modal.hide();
                        return Some(AppEvent::Exit);
                    }
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
                KeyCode::Left
                | KeyCode::Right
                | KeyCode::Char('h')
                | KeyCode::Char('l')
                | KeyCode::Up
                | KeyCode::Down
                | KeyCode::Char('j')
                | KeyCode::Char('k')
                    if on_body
                        && sort_tab
                        && self.sort_filter_modal.sort.focus == SortFocus::Order =>
                {
                    let s = &mut self.sort_filter_modal.sort;
                    match event.code {
                        KeyCode::Left | KeyCode::Char('h') | KeyCode::Up | KeyCode::Char('k') => {
                            s.ascending = true;
                        }
                        KeyCode::Right
                        | KeyCode::Char('l')
                        | KeyCode::Down
                        | KeyCode::Char('j') => {
                            s.ascending = false;
                        }
                        _ => {}
                    }
                    s.has_unapplied_changes = true;
                }
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
                    if self.pivot_melt_modal.focus == PivotMeltFocus::PivotAggregation {
                        self.pivot_melt_modal.pivot_move_aggregation_step(4, 0, -1);
                    } else if self.pivot_melt_modal.focus == PivotMeltFocus::PivotFilter {
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
                    if self.pivot_melt_modal.focus == PivotMeltFocus::PivotAggregation {
                        self.pivot_melt_modal.pivot_move_aggregation_step(4, 0, 1);
                    } else if self.pivot_melt_modal.focus == PivotMeltFocus::PivotFilter {
                        self.pivot_melt_modal
                            .pivot_filter_input
                            .handle_key(event, None);
                        self.pivot_melt_modal.pivot_index_table.select(None);
                    } else if self.pivot_melt_modal.focus == PivotMeltFocus::MeltFilter {
                        self.pivot_melt_modal
                            .melt_filter_input
                            .handle_key(event, None);
                        self.pivot_melt_modal.melt_index_table.select(None);
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
                KeyCode::Up | KeyCode::Char('k') if !pivot_melt_text_focus => {
                    match self.pivot_melt_modal.focus {
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
                            self.pivot_melt_modal.pivot_move_aggregation_step(4, -1, 0);
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
                    }
                }
                KeyCode::Down | KeyCode::Char('j') if !pivot_melt_text_focus => {
                    match self.pivot_melt_modal.focus {
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
                            self.pivot_melt_modal.pivot_move_aggregation_step(4, 1, 0);
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
                    }
                }
                KeyCode::Char(' ') if !pivot_melt_text_focus => match self.pivot_melt_modal.focus {
                    PivotMeltFocus::PivotIndexList => {
                        self.pivot_melt_modal.pivot_toggle_index_at_selection();
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
                        if let Some(ch) = self.pivot_melt_modal.melt_pattern[prev_byte..]
                            .chars()
                            .next()
                        {
                            self.pivot_melt_modal
                                .melt_pattern
                                .drain(prev_byte..prev_byte + ch.len_utf8());
                            self.pivot_melt_modal.melt_pattern_cursor -= 1;
                        }
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
                        if let Some(ch) = self.pivot_melt_modal.melt_pattern[byte_pos..]
                            .chars()
                            .next()
                        {
                            self.pivot_melt_modal
                                .melt_pattern
                                .drain(byte_pos..byte_pos + ch.len_utf8());
                        }
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
                        if let Some(ch) = self.pivot_melt_modal.melt_variable_name[prev_byte..]
                            .chars()
                            .next()
                        {
                            self.pivot_melt_modal
                                .melt_variable_name
                                .drain(prev_byte..prev_byte + ch.len_utf8());
                            self.pivot_melt_modal.melt_variable_cursor -= 1;
                        }
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
                        if let Some(ch) = self.pivot_melt_modal.melt_variable_name[byte_pos..]
                            .chars()
                            .next()
                        {
                            self.pivot_melt_modal
                                .melt_variable_name
                                .drain(byte_pos..byte_pos + ch.len_utf8());
                        }
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
                        if let Some(ch) = self.pivot_melt_modal.melt_value_name[prev_byte..]
                            .chars()
                            .next()
                        {
                            self.pivot_melt_modal
                                .melt_value_name
                                .drain(prev_byte..prev_byte + ch.len_utf8());
                            self.pivot_melt_modal.melt_value_cursor -= 1;
                        }
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
                        if let Some(ch) = self.pivot_melt_modal.melt_value_name[byte_pos..]
                            .chars()
                            .next()
                        {
                            self.pivot_melt_modal
                                .melt_value_name
                                .drain(byte_pos..byte_pos + ch.len_utf8());
                        }
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
                    KeyCode::Up | KeyCode::Char('k')
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
                    KeyCode::Down | KeyCode::Char('j')
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
                                let (width, height) = self.chart_export_modal.export_dimensions();
                                let mut path = PathBuf::from(path_str);
                                let format = self.chart_export_modal.selected_format;
                                // Only add default extension when user did not provide one
                                if path.extension().is_none() {
                                    path.set_extension(format.extension());
                                }
                                let path_display = path.display().to_string();
                                if path.exists() {
                                    self.pending_chart_export =
                                        Some((path, format, title, width, height));
                                    self.chart_export_modal.close();
                                    self.confirmation_modal.show(format!(
                                            "File already exists:\n{}\n\nDo you wish to overwrite this file?",
                                            path_display
                                        ));
                                } else {
                                    self.chart_export_modal.close();
                                    return Some(AppEvent::ChartExport(
                                        path, format, title, width, height,
                                    ));
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
                            } else if self.chart_export_modal.focus == ChartExportFocus::WidthInput
                            {
                                let allow = match event.code {
                                    KeyCode::Char(c) if c.is_ascii_digit() => true,
                                    KeyCode::Backspace
                                    | KeyCode::Delete
                                    | KeyCode::Left
                                    | KeyCode::Right
                                    | KeyCode::Home
                                    | KeyCode::End => true,
                                    _ => false,
                                };
                                if allow {
                                    let _ =
                                        self.chart_export_modal.width_input.handle_key(event, None);
                                }
                            } else if self.chart_export_modal.focus == ChartExportFocus::HeightInput
                            {
                                let allow = match event.code {
                                    KeyCode::Char(c) if c.is_ascii_digit() => true,
                                    KeyCode::Backspace
                                    | KeyCode::Delete
                                    | KeyCode::Left
                                    | KeyCode::Right
                                    | KeyCode::Home
                                    | KeyCode::End => true,
                                    _ => false,
                                };
                                if allow {
                                    let _ = self
                                        .chart_export_modal
                                        .height_input
                                        .handle_key(event, None);
                                }
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
                        match self.analysis_modal.selected_tool {
                            Some(analysis_modal::AnalysisTool::Describe) => {
                                self.analysis_modal.describe_results = None;
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
                            Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                self.analysis_modal.distribution_results = None;
                                self.analysis_modal.computing = Some(AnalysisProgress {
                                    phase: "Distribution".to_string(),
                                    current: 0,
                                    total: 1,
                                });
                                self.busy = true;
                                return Some(AppEvent::AnalysisDistributionCompute);
                            }
                            Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                self.analysis_modal.correlation_results = None;
                                self.analysis_modal.computing = Some(AnalysisProgress {
                                    phase: "Correlation".to_string(),
                                    current: 0,
                                    total: 1,
                                });
                                self.busy = true;
                                return Some(AppEvent::AnalysisCorrelationCompute);
                            }
                            None => {}
                        }
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
                            // Trigger computation for the selected tool when that tool has no cached results
                            match self.analysis_modal.selected_tool {
                                Some(analysis_modal::AnalysisTool::Describe)
                                    if self.analysis_modal.describe_results.is_none() =>
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
                                Some(analysis_modal::AnalysisTool::DistributionAnalysis)
                                    if self.analysis_modal.distribution_results.is_none() =>
                                {
                                    self.analysis_modal.computing = Some(AnalysisProgress {
                                        phase: "Distribution".to_string(),
                                        current: 0,
                                        total: 1,
                                    });
                                    self.busy = true;
                                    return Some(AppEvent::AnalysisDistributionCompute);
                                }
                                Some(analysis_modal::AnalysisTool::CorrelationMatrix)
                                    if self.analysis_modal.correlation_results.is_none() =>
                                {
                                    self.analysis_modal.computing = Some(AnalysisProgress {
                                        phase: "Correlation".to_string(),
                                        current: 0,
                                        total: 1,
                                    });
                                    self.busy = true;
                                    return Some(AppEvent::AnalysisCorrelationCompute);
                                }
                                _ => {}
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
                                                self.analysis_modal.current_results()
                                            {
                                                let max_rows = results.distribution_analyses.len();
                                                self.analysis_modal.next_row(max_rows);
                                            }
                                        }
                                        Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                            if let Some(results) =
                                                self.analysis_modal.current_results()
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
                                        if let Some(results) = self.analysis_modal.current_results()
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
                                        if let Some(results) = self.analysis_modal.current_results()
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
                                if let Some(results) = self.analysis_modal.current_results() {
                                    let max_rows = results.distribution_analyses.len();
                                    let page_size = 10;
                                    self.analysis_modal.page_down(max_rows, page_size);
                                }
                            }
                            Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                if let Some(results) = self.analysis_modal.current_results() {
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
                                        if let Some(results) = self.analysis_modal.current_results()
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
                                        if let Some(results) = self.analysis_modal.current_results()
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
                    self.template_modal.delete_confirm_focus =
                        !self.template_modal.delete_confirm_focus;
                }
                KeyCode::Left
                | KeyCode::Right
                | KeyCode::Up
                | KeyCode::Down
                | KeyCode::Char('h')
                | KeyCode::Char('l')
                | KeyCode::Char('j')
                | KeyCode::Char('k')
                    if self.template_modal.delete_confirm =>
                {
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
                                                let (query, sql_query, fuzzy_query) =
                                                    active_query_settings(
                                                        state.get_active_query(),
                                                        state.get_active_sql_query(),
                                                        state.get_active_fuzzy_query(),
                                                    );
                                                template.settings = template::TemplateSettings {
                                                    query,
                                                    sql_query,
                                                    fuzzy_query,
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
                KeyCode::Char('j')
                    if self.template_modal.mode == TemplateModalMode::List
                        && self.template_modal.focus == TemplateFocus::TemplateList
                        && !self.template_modal.delete_confirm =>
                {
                    let i = match self.template_modal.table_state.selected() {
                        Some(i) => {
                            if i >= self.template_modal.templates.len().saturating_sub(1) {
                                0
                            } else {
                                i + 1
                            }
                        }
                        None => 0,
                    };
                    self.template_modal.table_state.select(Some(i));
                }
                KeyCode::Char('k')
                    if self.template_modal.mode == TemplateModalMode::List
                        && self.template_modal.focus == TemplateFocus::TemplateList
                        && !self.template_modal.delete_confirm =>
                {
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
            if self.input_type == Some(InputType::Search) {
                const RIGHT_KEYS: [KeyCode; 2] = [KeyCode::Right, KeyCode::Char('l')];
                const LEFT_KEYS: [KeyCode; 2] = [KeyCode::Left, KeyCode::Char('h')];

                if self.query_focus == QueryFocus::TabBar && event.is_press() {
                    if event.code == KeyCode::BackTab
                        || (event.code == KeyCode::Tab
                            && !event.modifiers.contains(KeyModifiers::SHIFT))
                    {
                        self.query_focus = QueryFocus::Input;
                        if let Some(state) = &self.data_table_state {
                            if self.query_tab == QueryTab::SqlLike {
                                self.query_input.value = state.get_active_query().to_string();
                                self.query_input.cursor = self.query_input.value.chars().count();
                                self.sql_input.set_focused(false);
                                self.fuzzy_input.set_focused(false);
                                self.query_input.set_focused(true);
                            } else if self.query_tab == QueryTab::Fuzzy {
                                self.fuzzy_input.value = state.get_active_fuzzy_query().to_string();
                                self.fuzzy_input.cursor = self.fuzzy_input.value.chars().count();
                                self.query_input.set_focused(false);
                                self.sql_input.set_focused(false);
                                self.fuzzy_input.set_focused(true);
                            } else if self.query_tab == QueryTab::Sql {
                                self.sql_input.value = state.get_active_sql_query().to_string();
                                self.sql_input.cursor = self.sql_input.value.chars().count();
                                self.query_input.set_focused(false);
                                self.fuzzy_input.set_focused(false);
                                self.sql_input.set_focused(true);
                            }
                        }
                        return None;
                    }
                    if RIGHT_KEYS.contains(&event.code) {
                        self.query_tab = self.query_tab.next();
                        if let Some(state) = &self.data_table_state {
                            if self.query_tab == QueryTab::SqlLike {
                                self.query_input.value = state.get_active_query().to_string();
                                self.query_input.cursor = self.query_input.value.chars().count();
                            } else if self.query_tab == QueryTab::Fuzzy {
                                self.fuzzy_input.value = state.get_active_fuzzy_query().to_string();
                                self.fuzzy_input.cursor = self.fuzzy_input.value.chars().count();
                            } else if self.query_tab == QueryTab::Sql {
                                self.sql_input.value = state.get_active_sql_query().to_string();
                                self.sql_input.cursor = self.sql_input.value.chars().count();
                            }
                        }
                        self.query_input.set_focused(false);
                        self.sql_input.set_focused(false);
                        self.fuzzy_input.set_focused(false);
                        return None;
                    }
                    if LEFT_KEYS.contains(&event.code) {
                        self.query_tab = self.query_tab.prev();
                        if let Some(state) = &self.data_table_state {
                            if self.query_tab == QueryTab::SqlLike {
                                self.query_input.value = state.get_active_query().to_string();
                                self.query_input.cursor = self.query_input.value.chars().count();
                            } else if self.query_tab == QueryTab::Fuzzy {
                                self.fuzzy_input.value = state.get_active_fuzzy_query().to_string();
                                self.fuzzy_input.cursor = self.fuzzy_input.value.chars().count();
                            } else if self.query_tab == QueryTab::Sql {
                                self.sql_input.value = state.get_active_sql_query().to_string();
                                self.sql_input.cursor = self.sql_input.value.chars().count();
                            }
                        }
                        self.query_input.set_focused(false);
                        self.sql_input.set_focused(false);
                        self.fuzzy_input.set_focused(false);
                        return None;
                    }
                    if event.code == KeyCode::Esc {
                        self.query_input.clear();
                        self.sql_input.clear();
                        self.fuzzy_input.clear();
                        self.query_input.set_focused(false);
                        self.sql_input.set_focused(false);
                        self.fuzzy_input.set_focused(false);
                        self.input_mode = InputMode::Normal;
                        self.input_type = None;
                        if let Some(state) = &mut self.data_table_state {
                            state.error = None;
                            state.suppress_error_display = false;
                        }
                        return None;
                    }
                    return None;
                }

                if event.is_press()
                    && event.code == KeyCode::Tab
                    && !event.modifiers.contains(KeyModifiers::SHIFT)
                {
                    self.query_focus = QueryFocus::TabBar;
                    self.query_input.set_focused(false);
                    self.sql_input.set_focused(false);
                    self.fuzzy_input.set_focused(false);
                    return None;
                }

                if self.query_focus != QueryFocus::Input {
                    return None;
                }

                if self.query_tab == QueryTab::Sql {
                    self.query_input.set_focused(false);
                    self.fuzzy_input.set_focused(false);
                    self.sql_input.set_focused(true);
                    let result = self.sql_input.handle_key(event, Some(&self.cache));
                    match result {
                        TextInputEvent::Submit => {
                            let _ = self.sql_input.save_to_history(&self.cache);
                            let sql = self.sql_input.value.clone();
                            self.sql_input.set_focused(false);
                            return Some(AppEvent::SqlSearch(sql));
                        }
                        TextInputEvent::Cancel => {
                            self.sql_input.clear();
                            self.sql_input.set_focused(false);
                            self.input_mode = InputMode::Normal;
                            self.input_type = None;
                            if let Some(state) = &mut self.data_table_state {
                                state.error = None;
                                state.suppress_error_display = false;
                            }
                        }
                        TextInputEvent::HistoryChanged | TextInputEvent::None => {}
                    }
                    return None;
                }

                if self.query_tab == QueryTab::Fuzzy {
                    self.query_input.set_focused(false);
                    self.sql_input.set_focused(false);
                    self.fuzzy_input.set_focused(true);
                    let result = self.fuzzy_input.handle_key(event, Some(&self.cache));
                    match result {
                        TextInputEvent::Submit => {
                            let _ = self.fuzzy_input.save_to_history(&self.cache);
                            let query = self.fuzzy_input.value.clone();
                            self.fuzzy_input.set_focused(false);
                            return Some(AppEvent::FuzzySearch(query));
                        }
                        TextInputEvent::Cancel => {
                            self.fuzzy_input.clear();
                            self.fuzzy_input.set_focused(false);
                            self.input_mode = InputMode::Normal;
                            self.input_type = None;
                            if let Some(state) = &mut self.data_table_state {
                                state.error = None;
                                state.suppress_error_display = false;
                            }
                        }
                        TextInputEvent::HistoryChanged | TextInputEvent::None => {}
                    }
                    return None;
                }

                if self.query_tab != QueryTab::SqlLike {
                    return None;
                }

                self.sql_input.set_focused(false);
                self.fuzzy_input.set_focused(false);
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
                self.query_tab = QueryTab::SqlLike;
                self.query_focus = QueryFocus::Input;
                if let Some(state) = &mut self.data_table_state {
                    self.query_input.value = state.active_query.clone();
                    self.query_input.cursor = self.query_input.value.chars().count();
                    self.sql_input.value = state.get_active_sql_query().to_string();
                    self.fuzzy_input.value = state.get_active_fuzzy_query().to_string();
                    self.fuzzy_input.cursor = self.fuzzy_input.value.chars().count();
                    self.sql_input.cursor = self.sql_input.value.chars().count();
                    state.suppress_error_display = true;
                } else {
                    self.query_input.clear();
                    self.sql_input.clear();
                    self.fuzzy_input.clear();
                }
                self.sql_input.set_focused(false);
                self.fuzzy_input.set_focused(false);
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
                }
                None
            }
            KeyCode::Char('c') => {
                if let Some(state) = &self.data_table_state {
                    if self.input_mode == InputMode::Normal {
                        let numeric_columns: Vec<String> = state
                            .schema
                            .iter()
                            .filter(|(_, dtype)| dtype.is_numeric())
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
                // When busy (e.g. loading), still process column scroll, F1, and confirmation modal keys.
                if self.busy && !is_column_scroll && !is_help_key && !self.confirmation_modal.active
                {
                    return None;
                }
                self.key(key)
            }
            AppEvent::Open(paths, options) => {
                if paths.is_empty() {
                    return Some(AppEvent::Crash("No paths provided".to_string()));
                }
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
                let is_csv = options.format == Some(FileFormat::Csv)
                    || first
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
                        let size = Self::fetch_remote_size_http(url).unwrap_or(None);
                        let size_str = size
                            .map(Self::format_bytes)
                            .unwrap_or_else(|| "unknown".to_string());
                        let dest_dir = options
                            .temp_dir
                            .as_deref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|| std::env::temp_dir().display().to_string());
                        let message = format!(
                            "URL: {}\nFile size: {}\nDestination: {} (temporary file)\n\nContinue with download?",
                            url, size_str, dest_dir
                        );
                        self.pending_download = Some(PendingDownload::Http {
                            url: url.clone(),
                            size,
                            options: options.clone(),
                        });
                        self.confirmation_modal.show(message);
                        return None;
                    }
                    #[cfg(feature = "cloud")]
                    if let source::InputSource::S3(ref url) = src {
                        let full = format!("s3://{url}");
                        let (_, ext) = source::url_path_extension(&full);
                        let is_glob = full.contains('*') || full.ends_with('/');
                        if source::cloud_path_should_download(ext.as_deref(), is_glob) {
                            let size =
                                Self::fetch_remote_size_s3(&full, &self.app_config.cloud, options)
                                    .unwrap_or(None);
                            let size_str = size
                                .map(Self::format_bytes)
                                .unwrap_or_else(|| "unknown".to_string());
                            let dest_dir = options
                                .temp_dir
                                .as_deref()
                                .map(|p| p.display().to_string())
                                .unwrap_or_else(|| std::env::temp_dir().display().to_string());
                            let message = format!(
                                "URL: {}\nFile size: {}\nDestination: {} (temporary file)\n\nContinue with download?",
                                full, size_str, dest_dir
                            );
                            self.pending_download = Some(PendingDownload::S3 {
                                url: full,
                                size,
                                options: options.clone(),
                            });
                            self.confirmation_modal.show(message);
                            return None;
                        }
                    }
                    #[cfg(feature = "cloud")]
                    if let source::InputSource::Gcs(ref url) = src {
                        let full = format!("gs://{url}");
                        let (_, ext) = source::url_path_extension(&full);
                        let is_glob = full.contains('*') || full.ends_with('/');
                        if source::cloud_path_should_download(ext.as_deref(), is_glob) {
                            let size = Self::fetch_remote_size_gcs(&full, options).unwrap_or(None);
                            let size_str = size
                                .map(Self::format_bytes)
                                .unwrap_or_else(|| "unknown".to_string());
                            let dest_dir = options
                                .temp_dir
                                .as_deref()
                                .map(|p| p.display().to_string())
                                .unwrap_or_else(|| std::env::temp_dir().display().to_string());
                            let message = format!(
                                "URL: {}\nFile size: {}\nDestination: {} (temporary file)\n\nContinue with download?",
                                full, size_str, dest_dir
                            );
                            self.pending_download = Some(PendingDownload::Gcs {
                                url: full,
                                size,
                                options: options.clone(),
                            });
                            self.confirmation_modal.show(message);
                            return None;
                        }
                    }
                    let first = paths[0].clone();
                    // When CSV with --parse-strings, set "Scanning string columns" and defer build so UI can show it before blocking.
                    if paths.len() == 1 && is_csv && options.parse_strings.is_some() {
                        if let LoadingState::Loading {
                            file_path,
                            file_size,
                            ..
                        } = &self.loading_state
                        {
                            self.loading_state = LoadingState::Loading {
                                file_path: file_path.clone(),
                                file_size: *file_size,
                                current_phase: "Scanning string columns".to_string(),
                                progress_percent: 55,
                            };
                        }
                        return Some(AppEvent::DoLoadCsvWithParseStrings(
                            paths.clone(),
                            options.clone(),
                        ));
                    }
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
            AppEvent::DoLoadCsvWithParseStrings(paths, options) => {
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
                self.debug.schema_load = None;
                // Fast path for hive directory: infer schema from one parquet file instead of collect_schema() over all files.
                if options.single_spine_schema
                    && path.as_ref().is_some_and(|p| p.is_dir() && options.hive)
                {
                    let p = path.as_ref().expect("path set by caller");
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
                                    self.debug.schema_load = Some("one-file (local)".to_string());
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

                #[cfg(feature = "cloud")]
                {
                    // Use fast path for directory/glob cloud URLs (same as build_lazyframe_from_paths).
                    // Don't require --hive: path shape already implies hive scan.
                    if options.single_spine_schema
                        && path.as_ref().is_some_and(|p| {
                            let s = p.as_os_str().to_string_lossy();
                            let is_cloud = s.starts_with("s3://") || s.starts_with("gs://");
                            let looks_like_hive = s.ends_with('/') || s.contains('*');
                            is_cloud && (options.hive || looks_like_hive)
                        })
                    {
                        self.debug.schema_load = Some("trying one-file (cloud)".to_string());
                        let src = source::input_source(path.as_ref().expect("path set by caller"));
                        let try_cloud = match &src {
                            source::InputSource::S3(url) => {
                                let full = format!("s3://{url}");
                                let (path_part, _) = source::url_path_extension(&full);
                                let key = path_part
                                    .split_once('/')
                                    .map(|(_, k)| k.trim_end_matches('/'))
                                    .unwrap_or("");
                                let cloud_opts =
                                    Self::build_s3_cloud_options(&self.app_config.cloud, options);
                                Self::build_s3_object_store(&full, &self.app_config.cloud, options)
                                    .ok()
                                    .and_then(|store| {
                                        let rt = tokio::runtime::Runtime::new().ok()?;
                                        let (merged_schema, partition_columns) = rt
                                            .block_on(cloud_hive::schema_from_one_cloud_hive(
                                                store, key,
                                            ))
                                            .ok()?;
                                        let pl_path = PlRefPath::new(&full);
                                        let args = ScanArgsParquet {
                                            schema: Some(merged_schema.clone()),
                                            cloud_options: Some(cloud_opts),
                                            hive_options: polars::io::HiveOptions::new_enabled(),
                                            glob: true,
                                            ..Default::default()
                                        };
                                        let mut lf_owned =
                                            LazyFrame::scan_parquet(pl_path, args).ok()?;
                                        if !partition_columns.is_empty() {
                                            let exprs: Vec<_> = partition_columns
                                                .iter()
                                                .map(|s| col(s.as_str()))
                                                .chain(
                                                    merged_schema
                                                        .iter_names()
                                                        .map(|s| s.to_string())
                                                        .filter(|c| !partition_columns.contains(c))
                                                        .map(|s| col(s.as_str())),
                                                )
                                                .collect();
                                            lf_owned = lf_owned.select(exprs);
                                        }
                                        DataTableState::from_schema_and_lazyframe(
                                            merged_schema,
                                            lf_owned,
                                            options,
                                            Some(partition_columns),
                                        )
                                        .ok()
                                    })
                            }
                            source::InputSource::Gcs(url) => {
                                let full = format!("gs://{url}");
                                let (path_part, _) = source::url_path_extension(&full);
                                let key = path_part
                                    .split_once('/')
                                    .map(|(_, k)| k.trim_end_matches('/'))
                                    .unwrap_or("");
                                Self::build_gcs_object_store(&full).ok().and_then(|store| {
                                    let rt = tokio::runtime::Runtime::new().ok()?;
                                    let (merged_schema, partition_columns) = rt
                                        .block_on(cloud_hive::schema_from_one_cloud_hive(
                                            store, key,
                                        ))
                                        .ok()?;
                                    let pl_path = PlRefPath::new(&full);
                                    let args = ScanArgsParquet {
                                        schema: Some(merged_schema.clone()),
                                        cloud_options: Some(CloudOptions::default()),
                                        hive_options: polars::io::HiveOptions::new_enabled(),
                                        glob: true,
                                        ..Default::default()
                                    };
                                    let mut lf_owned =
                                        LazyFrame::scan_parquet(pl_path, args).ok()?;
                                    if !partition_columns.is_empty() {
                                        let exprs: Vec<_> = partition_columns
                                            .iter()
                                            .map(|s| col(s.as_str()))
                                            .chain(
                                                merged_schema
                                                    .iter_names()
                                                    .map(|s| s.to_string())
                                                    .filter(|c| !partition_columns.contains(c))
                                                    .map(|s| col(s.as_str())),
                                            )
                                            .collect();
                                        lf_owned = lf_owned.select(exprs);
                                    }
                                    DataTableState::from_schema_and_lazyframe(
                                        merged_schema,
                                        lf_owned,
                                        options,
                                        Some(partition_columns),
                                    )
                                    .ok()
                                })
                            }
                            _ => None,
                        };
                        if let Some(state) = try_cloud {
                            self.debug.schema_load = Some("one-file (cloud)".to_string());
                            self.parquet_metadata_cache = None;
                            self.export_df = None;
                            self.data_table_state = Some(state);
                            self.path = path.clone();
                            if let Some(ref path_p) = path {
                                self.original_file_format =
                                    path_p.extension().and_then(|e| e.to_str()).and_then(|ext| {
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
                        } else {
                            self.debug.schema_load = Some("fallback (cloud)".to_string());
                        }
                    }
                }

                if self.debug.schema_load.is_none() {
                    self.debug.schema_load = Some("full scan".to_string());
                }
                let mut lf_owned = (**lf).clone();
                match lf_owned.collect_schema() {
                    Ok(schema) => {
                        let partition_columns = if path.as_ref().is_some_and(|p| {
                            options.hive
                                && (p.is_dir() || p.as_os_str().to_string_lossy().contains('*'))
                        }) {
                            let discovered = DataTableState::discover_hive_partition_columns(
                                path.as_ref().expect("path set by caller"),
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
                        None => match crate::statistics::collect_lazy(
                            lf.clone().select([len()]),
                            self.app_config.performance.polars_streaming,
                        ) {
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
                        self.app_config.performance.polars_streaming,
                    ) {
                        Ok(results) => {
                            self.analysis_modal.describe_results = Some(results);
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
                    let options = crate::statistics::ComputeOptions {
                        include_distribution_info: true,
                        include_distribution_analyses: true,
                        include_correlation_matrix: false,
                        include_skewness_kurtosis_outliers: true,
                        polars_streaming: self.app_config.performance.polars_streaming,
                    };
                    if let Ok(results) = crate::statistics::compute_statistics_with_options(
                        &state.lf,
                        self.sampling_threshold,
                        self.analysis_modal.random_seed,
                        options,
                    ) {
                        self.analysis_modal.distribution_results = Some(results);
                    }
                }
                self.analysis_modal.computing = None;
                self.busy = false;
                self.drain_keys_on_next_loop = true;
                None
            }
            AppEvent::AnalysisCorrelationCompute => {
                if let Some(state) = &self.data_table_state {
                    if let Ok(df) =
                        crate::statistics::collect_lazy(state.lf.clone(), state.polars_streaming)
                    {
                        if let Ok(matrix) = crate::statistics::compute_correlation_matrix(&df) {
                            self.analysis_modal.correlation_results =
                                Some(crate::statistics::AnalysisResults {
                                    column_statistics: vec![],
                                    total_rows: df.height(),
                                    sample_size: None,
                                    sample_seed: self.analysis_modal.random_seed,
                                    correlation_matrix: Some(matrix),
                                    distribution_analyses: vec![],
                                });
                        }
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
            AppEvent::SqlSearch(sql) => {
                let sql_succeeded = if let Some(state) = &mut self.data_table_state {
                    state.sql_query(sql.clone());
                    state.error.is_none()
                } else {
                    false
                };
                if sql_succeeded {
                    self.input_mode = InputMode::Normal;
                    self.sql_input.set_focused(false);
                    if let Some(state) = &mut self.data_table_state {
                        state.suppress_error_display = false;
                    }
                    Some(AppEvent::Collect)
                } else {
                    None
                }
            }
            AppEvent::FuzzySearch(query) => {
                let fuzzy_succeeded = if let Some(state) = &mut self.data_table_state {
                    state.fuzzy_search(query.clone());
                    state.error.is_none()
                } else {
                    false
                };
                if fuzzy_succeeded {
                    self.input_mode = InputMode::Normal;
                    self.fuzzy_input.set_focused(false);
                    if let Some(state) = &mut self.data_table_state {
                        state.suppress_error_display = false;
                    }
                    Some(AppEvent::Collect)
                } else {
                    None
                }
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
            AppEvent::ChartExport(path, format, title, width, height) => {
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
                    *width,
                    *height,
                ))
            }
            AppEvent::DoChartExport(path, format, title, width, height) => {
                let result = self.do_chart_export(path, *format, title, *width, *height);
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
                    match crate::statistics::collect_lazy(state.lf.clone(), state.polars_streaming)
                    {
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
    /// Width and height are used for PNG output (pixels); EPS uses fixed logical size.
    fn do_chart_export(
        &self,
        path: &Path,
        format: ChartExportFormat,
        title: &str,
        width: u32,
        height: u32,
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
                    if let Some(cache) = self.chart_cache.xy.as_ref() {
                        let pts = if self.chart_modal.log_scale {
                            cache.series_log.as_ref().cloned().unwrap_or_else(|| {
                                cache
                                    .series
                                    .iter()
                                    .map(|s| {
                                        s.iter().map(|&(x, y)| (x, y.max(0.0).ln_1p())).collect()
                                    })
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
                    }
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
                    ChartExportFormat::Png => {
                        write_chart_png(path, &series, chart_type, &bounds, (width, height))
                    }
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
                        write_chart_png(path, &series, ChartType::Bar, &bounds, (width, height))
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
                    ChartExportFormat::Png => {
                        write_box_plot_png(path, &data, &bounds, (width, height))
                    }
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
                        write_chart_png(path, &series, ChartType::Line, &bounds, (width, height))
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
                    ChartExportFormat::Png => {
                        write_heatmap_png(path, &data, &bounds, (width, height))
                    }
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
                active_sql_query: state.get_active_sql_query().to_string(),
                active_fuzzy_query: state.get_active_fuzzy_query().to_string(),
                filters: state.get_filters().to_vec(),
                sort_columns: state.get_sort_columns().to_vec(),
                sort_ascending: state.get_sort_ascending(),
                column_order: state.get_column_order().to_vec(),
                locked_columns_count: state.locked_columns_count(),
            });
        let saved_active_template_id = self.active_template_id.clone();

        if let Some(state) = &mut self.data_table_state {
            state.error = None;

            // At most one of SQL or DSL query is stored per template; then fuzzy. Apply in that order.
            let sql_trimmed = template.settings.sql_query.as_deref().unwrap_or("").trim();
            let query_opt = template.settings.query.as_deref().filter(|s| !s.is_empty());
            let fuzzy_trimmed = template
                .settings
                .fuzzy_query
                .as_deref()
                .unwrap_or("")
                .trim();

            if !sql_trimmed.is_empty() {
                state.sql_query(template.settings.sql_query.clone().unwrap_or_default());
            } else if let Some(q) = query_opt {
                state.query(q.to_string());
            }
            if let Some(error) = state.error.clone() {
                if let Some(saved) = saved_state {
                    self.restore_state(saved);
                }
                self.active_template_id = saved_active_template_id;
                return Err(color_eyre::eyre::eyre!(
                    "{}",
                    crate::error_display::user_message_from_polars(&error)
                ));
            }

            if !fuzzy_trimmed.is_empty() {
                state.fuzzy_search(template.settings.fuzzy_query.clone().unwrap_or_default());
                if let Some(error) = state.error.clone() {
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
        let mut df = crate::statistics::collect_lazy(state.lf.clone(), state.polars_streaming)?;
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
            state.active_sql_query = saved.active_sql_query;
            state.active_fuzzy_query = saved.active_fuzzy_query;
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
            let (query, sql_query, fuzzy_query) = active_query_settings(
                state.get_active_query(),
                state.get_active_sql_query(),
                state.get_active_fuzzy_query(),
            );
            template::TemplateSettings {
                query,
                sql_query,
                fuzzy_query,
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
                sql_query: None,
                fuzzy_query: None,
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

        use crate::render::context::RenderContext;
        use crate::render::layout::{app_layout, centered_rect_loading};
        use crate::render::main_view::MainViewContent;

        let ctx = RenderContext::from_theme_and_config(
            &self.theme,
            self.table_cell_padding,
            self.column_colors,
        );

        let main_view_content = MainViewContent::from_app_state(
            self.analysis_modal.active,
            self.input_mode == InputMode::Chart,
        );

        Clear.render(area, buf);
        let background_color = self.color("background");
        Block::default()
            .style(Style::default().bg(background_color))
            .render(area, buf);

        let app_layout = app_layout(area, self.debug.enabled);
        let main_area = app_layout.main_view;
        Clear.render(main_area, buf);

        crate::render::main_view_render::render_main_view(area, main_area, buf, self, &ctx);

        // Render loading progress popover (min 25 chars wide, max 25% of area; throbber spins via busy in controls)
        if matches!(self.loading_state, LoadingState::Loading { .. }) {
            if let LoadingState::Loading {
                current_phase,
                progress_percent,
                ..
            } = &self.loading_state
            {
                let popover_rect = centered_rect_loading(area);
                crate::render::overlays::render_loading_gauge(
                    popover_rect,
                    buf,
                    "Loading",
                    current_phase,
                    *progress_percent,
                    ctx.modal_border,
                    ctx.primary_chart_series_color,
                );
            }
        }
        if matches!(self.loading_state, LoadingState::Exporting { .. }) {
            if let LoadingState::Exporting {
                file_path,
                current_phase,
                progress_percent,
            } = &self.loading_state
            {
                let label = format!("{}: {}", current_phase, file_path.display());
                crate::render::overlays::render_loading_gauge(
                    area,
                    buf,
                    "Exporting",
                    &label,
                    *progress_percent,
                    ctx.modal_border,
                    ctx.primary_chart_series_color,
                );
            }
        }

        if self.confirmation_modal.active {
            crate::render::overlays::render_confirmation_modal(
                area,
                buf,
                &self.confirmation_modal,
                &ctx,
            );
        }
        if self.success_modal.active {
            crate::render::overlays::render_success_modal(area, buf, &self.success_modal, &ctx);
        }
        if self.error_modal.active {
            crate::render::overlays::render_error_modal(area, buf, &self.error_modal, &ctx);
        }
        if self.show_help
            || (self.template_modal.active && self.template_modal.show_help)
            || (self.analysis_modal.active && self.analysis_modal.show_help)
        {
            let (title, text): (String, String) =
                if self.analysis_modal.active && self.analysis_modal.show_help {
                    crate::render::analysis_view::help_title_and_text(&self.analysis_modal)
                } else if self.template_modal.active {
                    (
                        "Template Help".to_string(),
                        help_strings::template().to_string(),
                    )
                } else {
                    let (t, txt) = self.get_help_info();
                    (t.to_string(), txt.to_string())
                };
            crate::render::overlays::render_help_overlay(
                area,
                buf,
                &title,
                &text,
                &mut self.help_scroll,
                &ctx,
            );
        }

        let row_count = self.data_table_state.as_ref().map(|s| s.num_rows);
        let use_unicode_throbber = std::env::var("LANG")
            .map(|l| l.to_uppercase().contains("UTF-8"))
            .unwrap_or(false);
        let mut controls = Controls::from_context(row_count.unwrap_or(0), &ctx)
            .with_unicode_throbber(use_unicode_throbber);

        match crate::render::main_view::control_bar_spec(self, main_view_content) {
            crate::render::main_view::ControlBarSpec::Datatable {
                dimmed,
                query_active,
            } => {
                controls = controls.with_dimmed(dimmed).with_query_active(query_active);
            }
            crate::render::main_view::ControlBarSpec::Custom(pairs) => {
                controls = controls.with_custom_controls(pairs);
            }
        }

        if self.busy {
            self.throbber_frame = self.throbber_frame.wrapping_add(1);
        }
        controls = controls.with_busy(self.busy, self.throbber_frame);
        controls.render(app_layout.control_bar, buf);
        if let Some(debug_area) = app_layout.debug {
            self.debug.render(debug_area, buf);
        }
    }
}

/// Run the TUI with either file paths or an existing LazyFrame. Single event loop used by CLI and Python binding.
pub fn run(input: RunInput, config: Option<AppConfig>) -> Result<()> {
    use std::io::Write;
    use std::sync::{mpsc, Mutex, Once};

    let config = match config {
        Some(c) => c,
        None => AppConfig::load(APP_NAME)?,
    };

    let opts = match &input {
        RunInput::Paths(_, o) => o.clone(),
        RunInput::LazyFrame(_, o) => o.clone(),
    };

    let theme = Theme::from_config(&config.theme)
        .or_else(|e| Theme::from_config(&AppConfig::default().theme).map_err(|_| e))?;

    // Install color_eyre at most once per process (e.g. first datui.view() in Python).
    // Subsequent run() calls skip install and reuse the result; no error-message detection.
    static COLOR_EYRE_INIT: Once = Once::new();
    static INSTALL_RESULT: Mutex<Option<Result<(), color_eyre::Report>>> = Mutex::new(None);
    COLOR_EYRE_INIT.call_once(|| {
        *INSTALL_RESULT.lock().unwrap_or_else(|e| e.into_inner()) = Some(color_eyre::install());
    });
    if let Some(Err(e)) = INSTALL_RESULT
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
    {
        return Err(color_eyre::eyre::eyre!(e.to_string()));
    }
    // Require at least one path so event handlers can safely use paths[0].
    if let RunInput::Paths(ref paths, _) = input {
        if paths.is_empty() {
            return Err(color_eyre::eyre::eyre!("At least one path is required"));
        }
        for path in paths {
            let s = path.to_string_lossy();
            let is_remote = s.starts_with("s3://")
                || s.starts_with("gs://")
                || s.starts_with("http://")
                || s.starts_with("https://");
            let is_glob = s.contains('*');
            if !is_remote && !is_glob && !path.exists() {
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
    if opts.debug {
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
                crossterm::event::Event::Key(key) => {
                    if key.is_press() {
                        tx.send(AppEvent::Key(key))?
                    }
                }
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
