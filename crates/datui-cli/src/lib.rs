//! Shared CLI definitions for datui.
//!
//! Used by the main application and by the build script (manpage) and
//! gen_docs binary (command-line-options markdown).

use clap::{CommandFactory, Parser, ValueEnum};
use std::path::Path;

/// File format for data files (used to bypass extension-based detection).
/// When `--format` is not specified, format is auto-detected from the file extension.
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum FileFormat {
    /// Parquet columnar format
    Parquet,
    /// Comma-separated values
    Csv,
    /// Tab-separated values
    Tsv,
    /// Pipe-separated values
    Psv,
    /// JSON array format
    Json,
    /// JSON Lines / NDJSON (one JSON object per line)
    Jsonl,
    /// Arrow IPC / Feather
    Arrow,
    /// Avro row format
    Avro,
    /// ORC columnar format
    Orc,
    /// Excel (.xls, .xlsx, .xlsm, .xlsb)
    Excel,
}

impl FileFormat {
    /// Detect file format from path extension. Returns None when extension is missing or unknown.
    pub fn from_path(path: &Path) -> Option<Self> {
        path.extension()
            .and_then(|e| e.to_str())
            .and_then(Self::from_extension)
    }

    /// Parse format from extension string (e.g. "parquet", "csv").
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_lowercase().as_str() {
            "parquet" => Some(Self::Parquet),
            "csv" => Some(Self::Csv),
            "tsv" => Some(Self::Tsv),
            "psv" => Some(Self::Psv),
            "json" => Some(Self::Json),
            "jsonl" | "ndjson" => Some(Self::Jsonl),
            "arrow" | "ipc" | "feather" => Some(Self::Arrow),
            "avro" => Some(Self::Avro),
            "orc" => Some(Self::Orc),
            "xls" | "xlsx" | "xlsm" | "xlsb" => Some(Self::Excel),
            _ => None,
        }
    }
}

/// Compression format for data files
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum CompressionFormat {
    /// Gzip compression (.gz) - Most common, good balance of speed and compression
    Gzip,
    /// Zstandard compression (.zst) - Modern, fast compression with good ratios
    Zstd,
    /// Bzip2 compression (.bz2) - Good compression ratio, slower than gzip
    Bzip2,
    /// XZ compression (.xz) - Excellent compression ratio, slower than bzip2
    Xz,
}

impl CompressionFormat {
    /// Detect compression format from file extension
    pub fn from_extension(path: &Path) -> Option<Self> {
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

/// Command-line arguments for datui
#[derive(Clone, Parser, Debug)]
#[command(
    name = "datui",
    version,
    about = "Data Exploration in the Terminal",
    long_about = include_str!("../long_about.txt")
)]
pub struct Args {
    /// Path(s) to the data file(s) to open.
    /// Multiple files of the same format are concatenated into one table (not required with --generate-config, --clear-cache, or --remove-templates)
    #[arg(required_unless_present_any = ["generate_config", "clear_cache", "remove_templates"], num_args = 1.., value_name = "PATH")]
    pub paths: Vec<std::path::PathBuf>,

    /// Skip this many lines when reading a file
    #[arg(long = "skip-lines")]
    pub skip_lines: Option<usize>,

    /// Skip this many rows when reading a file
    #[arg(long = "skip-rows")]
    pub skip_rows: Option<usize>,

    /// Skip this many rows at the end of the file (e.g. to ignore vendor footer or trailing garbage)
    #[arg(long = "skip-tail-rows", value_name = "N")]
    pub skip_tail_rows: Option<usize>,

    /// Specify that the file has no header
    #[arg(long = "no-header")]
    pub no_header: Option<bool>,

    /// Specify the delimiter to use when reading a delimited text file
    #[arg(long = "delimiter")]
    pub delimiter: Option<u8>,

    /// Number of rows to use when inferring CSV schema (default: 1000). Larger values reduce risk of wrong type (e.g. int then N/A).
    #[arg(long = "infer-schema-length", value_name = "N")]
    pub infer_schema_length: Option<usize>,

    /// When reading CSV, ignore parse errors and continue with the next batch (default: false)
    #[arg(long = "ignore-errors", value_name = "BOOL", value_parser = clap::value_parser!(bool))]
    pub ignore_errors: Option<bool>,

    /// Treat these values as null when reading CSV. Use once per value; no "=" means all columns, COL=VAL means column COL only (first "=" separates column from value). Example: --null-value NA --null-value amount=
    #[arg(long = "null-value", value_name = "VAL")]
    pub null_value: Vec<String>,

    /// Specify the compression format explicitly (gzip, zstd, bzip2, xz)
    /// If not specified, compression is auto-detected from file extension.
    #[arg(long = "compression", value_enum)]
    pub compression: Option<CompressionFormat>,

    /// Force file format (parquet, csv, tsv, psv, json, jsonl, arrow, avro, orc, excel).
    /// By default format is auto-detected from the file extension. Use this for URLs or paths without an extension.
    #[arg(long = "format", value_enum)]
    pub format: Option<FileFormat>,

    /// Enable debug mode to show operational information
    #[arg(long = "debug", action)]
    pub debug: bool,

    /// Enable Hive-style partitioning for directory or glob paths; ignored for a single file
    #[arg(long = "hive", action)]
    pub hive: bool,

    /// Infer Hive/partitioned Parquet schema from one file for faster load (default: true). Set to false to use full schema scan.
    #[arg(long = "single-spine-schema", value_name = "BOOL", value_parser = clap::value_parser!(bool))]
    pub single_spine_schema: Option<bool>,

    /// Try to parse CSV string columns as dates (e.g. YYYY-MM-DD, ISO datetime). Default: true
    #[arg(long = "parse-dates", value_name = "BOOL", value_parser = clap::value_parser!(bool))]
    pub parse_dates: Option<bool>,

    /// Trim whitespace and parse CSV string columns as date, datetime, time, duration, int, or float. Default: applied to all string columns. Use --parse-strings=COL (repeatable) to limit to specific columns, or --no-parse-strings to disable.
    #[arg(long = "parse-strings", value_name = "COL", num_args = 0.., default_missing_value = "")]
    pub parse_strings: Vec<String>,

    /// Disable parse-strings for CSV (trim and type inference). Overrides config and default.
    #[arg(long = "no-parse-strings", action)]
    pub no_parse_strings: bool,

    /// Decompress into memory. Default: decompress to temp file and use lazy scan
    #[arg(long = "decompress-in-memory", default_missing_value = "true", num_args = 0..=1, value_parser = clap::value_parser!(bool))]
    pub decompress_in_memory: Option<bool>,

    /// Directory for decompression temp files (default: system temp, e.g. TMPDIR)
    #[arg(long = "temp-dir", value_name = "DIR")]
    pub temp_dir: Option<std::path::PathBuf>,

    /// Excel sheet to load: 0-based index (e.g. 0) or sheet name (e.g. "Sales")
    #[arg(long = "sheet", value_name = "SHEET")]
    pub excel_sheet: Option<String>,

    /// Clear all cache data and exit
    #[arg(long = "clear-cache", action)]
    pub clear_cache: bool,

    /// Apply a template by name when starting the application
    #[arg(long = "template")]
    pub template: Option<String>,

    /// Remove all templates and exit
    #[arg(long = "remove-templates", action)]
    pub remove_templates: bool,

    /// When set, datasets with this many or more rows are sampled for analysis (faster, less memory).
    /// Overrides config [performance] sampling_threshold. Use 0 to disable sampling (full dataset) for this run.
    /// When omitted, config or full-dataset mode is used.
    #[arg(long = "sampling-threshold", value_name = "N")]
    pub sampling_threshold: Option<usize>,

    /// Use Polars streaming engine for LazyFrame collect when available (default: true). Set to false to disable.
    #[arg(long = "polars-streaming", value_name = "BOOL", value_parser = clap::value_parser!(bool))]
    pub polars_streaming: Option<bool>,

    /// Apply workaround for Polars 0.52 pivot with Date/Datetime index (default: true). Set to false to test without it.
    #[arg(long = "workaround-pivot-date-index", value_name = "BOOL", value_parser = clap::value_parser!(bool))]
    pub workaround_pivot_date_index: Option<bool>,

    /// Number of pages to buffer ahead of the visible area (default: 3)
    /// Larger values provide smoother scrolling but use more memory
    #[arg(long = "pages-lookahead")]
    pub pages_lookahead: Option<usize>,

    /// Number of pages to buffer behind the visible area (default: 3)
    /// Larger values provide smoother scrolling but use more memory
    #[arg(long = "pages-lookback")]
    pub pages_lookback: Option<usize>,

    /// Display row numbers on the left side of the table
    #[arg(long = "row-numbers", action)]
    pub row_numbers: bool,

    /// Starting index for row numbers (default: 1)
    #[arg(long = "row-start-index")]
    pub row_start_index: Option<usize>,

    /// Colorize main table cells by column type (default: true). Set to false to disable.
    #[arg(long = "column-colors", value_name = "BOOL", value_parser = clap::value_parser!(bool))]
    pub column_colors: Option<bool>,

    /// Generate default configuration file at ~/.config/datui/config.toml
    #[arg(long = "generate-config", action)]
    pub generate_config: bool,

    /// Force overwrite existing config file when using --generate-config
    #[arg(long = "force", requires = "generate_config", action)]
    pub force: bool,

    /// S3-compatible endpoint URL (overrides config and AWS_ENDPOINT_URL). Example: http://localhost:9000
    #[arg(long = "s3-endpoint-url", value_name = "URL")]
    pub s3_endpoint_url: Option<String>,

    /// S3 access key (overrides config and AWS_ACCESS_KEY_ID)
    #[arg(long = "s3-access-key-id", value_name = "KEY")]
    pub s3_access_key_id: Option<String>,

    /// S3 secret key (overrides config and AWS_SECRET_ACCESS_KEY)
    #[arg(long = "s3-secret-access-key", value_name = "SECRET")]
    pub s3_secret_access_key: Option<String>,

    /// S3 region (overrides config and AWS_REGION). Example: us-east-1
    #[arg(long = "s3-region", value_name = "REGION")]
    pub s3_region: Option<String>,
}

/// Escape `|` and newlines for use in markdown table cells.
fn escape_table_cell(s: &str) -> String {
    s.replace('|', "\\|").replace(['\n', '\r'], " ")
}

/// Render command-line options as markdown.
///
/// Used by the gen_docs binary; output is written to stdout and then
/// to `docs/reference/command-line-options.md` by the docs build process.
pub fn render_options_markdown() -> String {
    let mut cmd = Args::command();
    cmd.build();

    let mut out = String::from("# Command Line Options\n\n");

    out.push_str("## Usage\n\n```\n");
    let usage = cmd.render_usage();
    out.push_str(&usage.to_string());
    out.push_str("\n```\n\n");

    out.push_str("## Options\n\n");
    out.push_str("| Option | Description |\n");
    out.push_str("|--------|-------------|\n");

    for arg in cmd.get_arguments() {
        let id = arg.get_id().as_ref().to_string();
        if id == "help" || id == "version" {
            continue;
        }

        let option_str = if arg.is_positional() {
            let placeholder: String = arg
                .get_value_names()
                .map(|names| {
                    names
                        .iter()
                        .map(|n: &clap::builder::Str| format!("<{}>", n.as_ref() as &str))
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .unwrap_or_default();
            if arg.is_required_set() {
                placeholder
            } else {
                format!("[{placeholder}]")
            }
        } else {
            let mut parts = Vec::new();
            if let Some(s) = arg.get_short() {
                parts.push(format!("-{s}"));
            }
            if let Some(l) = arg.get_long() {
                parts.push(format!("--{l}"));
            }
            let op = parts.join(", ");
            let takes_val = arg.get_action().takes_values();
            let placeholder: String = if takes_val {
                arg.get_value_names()
                    .map(|names| {
                        names
                            .iter()
                            .map(|n: &clap::builder::Str| format!("<{}>", n.as_ref() as &str))
                            .collect::<Vec<_>>()
                            .join(" ")
                    })
                    .unwrap_or_default()
            } else {
                String::new()
            };
            if placeholder.is_empty() {
                op
            } else {
                format!("{op} {placeholder}")
            }
        };

        let help = arg
            .get_help()
            .map(|h| escape_table_cell(&h.to_string()))
            .unwrap_or_else(|| "-".to_string());

        out.push_str(&format!("| `{option_str}` | {help} |\n"));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_file_format_from_path() {
        assert_eq!(
            FileFormat::from_path(Path::new("data.parquet")),
            Some(FileFormat::Parquet)
        );
        assert_eq!(
            FileFormat::from_path(Path::new("data.csv")),
            Some(FileFormat::Csv)
        );
        assert_eq!(
            FileFormat::from_path(Path::new("file.jsonl")),
            Some(FileFormat::Jsonl)
        );
        assert_eq!(FileFormat::from_path(Path::new("noext")), None);
        assert_eq!(
            FileFormat::from_path(Path::new("file.NDJSON")),
            Some(FileFormat::Jsonl)
        );
    }
}
