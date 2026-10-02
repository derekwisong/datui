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
    /// SafeTensors model weights: the tensor list, read from the header
    Safetensors,
    /// GGUF model weights: the tensor list, read from the header
    Gguf,
}

impl FileFormat {
    /// Detect file format from path extension. Returns None when extension is missing or unknown.
    ///
    /// A sharded checkpoint's `model.safetensors.index.json` is SafeTensors: it is read
    /// as the shards it names, not as JSON.
    pub fn from_path(path: &Path) -> Option<Self> {
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.to_ascii_lowercase().ends_with(".safetensors.index.json"))
        {
            return Some(Self::Safetensors);
        }
        path.extension()
            .and_then(|e| e.to_str())
            .and_then(Self::from_extension)
    }

    /// The format's name, as a row on the home screen says it: `12 parquet`, `3 csv`.
    ///
    /// Lowercase and singular, because it is counted beside a number and read as a
    /// noun. One name per format rather than per extension: `.ipc`, `.arrow` and
    /// `.feather` are `arrow`, which is what a reader has to know about them.
    pub fn name(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Csv => "csv",
            Self::Tsv => "tsv",
            Self::Psv => "psv",
            Self::Json => "json",
            Self::Jsonl => "jsonl",
            Self::Arrow => "arrow",
            Self::Avro => "avro",
            Self::Orc => "orc",
            Self::Excel => "excel",
            Self::Safetensors => "safetensors",
            Self::Gguf => "gguf",
        }
    }

    /// Every format, for the places that have to consider all of them.
    ///
    /// Written out, and so able to fall behind the enum. `every_format_is_listed`
    /// matches a variant exhaustively, so adding one stops that test compiling — which
    /// is a reminder at the right moment rather than a guarantee, since the author
    /// could extend the match and leave this list short. What that would cost is
    /// bounded: `from_name` answers `None` for the new format, and every caller reads
    /// `None` as "not Parquet", which is the direction that leaves counts off a directory
    /// rather than giving it another format's.
    pub const ALL: [Self; 12] = [
        Self::Parquet,
        Self::Csv,
        Self::Tsv,
        Self::Psv,
        Self::Json,
        Self::Jsonl,
        Self::Arrow,
        Self::Avro,
        Self::Orc,
        Self::Excel,
        Self::Safetensors,
        Self::Gguf,
    ];

    /// The format a [`FileFormat::name`] names, for a name that was stored rather than
    /// carried. The inverse of that method, and the only way back: a name is not an
    /// extension, so `from_extension` cannot read one.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|f| f.name() == name)
    }

    /// Whether many files of this format can be read as one table.
    ///
    /// Asked before a directory is offered as a dataset, so the home screen cannot
    /// promise an open the reader has no route for. The open's own refusal is a match arm
    /// in `datui-lib`, which asserts against this predicate on every debug run, so the
    /// two cannot name different formats without a test saying so.
    ///
    /// Tsv and Psv have a single-file reader and no multi-path one; an Excel workbook
    /// is sheets rather than rows, with nothing to concatenate. Reading the first two
    /// as a list is #275 phase 4's ("never refuse").
    pub fn reads_many_files(self) -> bool {
        !matches!(self, Self::Tsv | Self::Psv | Self::Excel)
    }

    /// The column separator a delimited format is read with when `--delimiter` is not
    /// given. `None` for the formats that are not delimited text.
    pub fn separator(self) -> Option<u8> {
        match self {
            Self::Csv => Some(b','),
            Self::Tsv => Some(b'\t'),
            Self::Psv => Some(b'|'),
            _ => None,
        }
    }

    /// Parse format from extension string (e.g. "parquet", "csv").
    ///
    /// The one place an extension becomes a format. Everything that asks whether a name
    /// is data — the home screen, the search, `~` path input, the CLI and the cloud
    /// listings — asks here, so no route can offer a file another route cannot open.
    ///
    /// `.txt` is deliberately absent. It was listed as data and had no reader, so a
    /// `README.txt` was offered on the home screen and refused when opened. Giving it
    /// one is worse: a README beside two Parquet files would make the directory two
    /// formats and stop it opening at all. A genuinely tabular `.txt` opens with
    /// `--format csv`.
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
            "safetensors" => Some(Self::Safetensors),
            "gguf" => Some(Self::Gguf),
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

/// Accepted values for `--number-format`.
///
/// This crate cannot depend on datui-lib (the dependency runs the other way),
/// so the list is duplicated here to give clap proper `--help` output and shell
/// completion. `number_format_values_match_presets` in datui-lib asserts the two
/// lists stay in sync.
pub const NUMBER_FORMAT_VALUES: &[&str] = &[
    "none",
    "thousands",
    "european",
    "si",
    "swiss",
    "indian",
    "underscore",
    "system",
];

/// The examples shown after `--help`, in the manpage and in the CLI reference.
pub const EXAMPLES: &str = include_str!("../examples.txt");

/// One entry of [`EXAMPLES`]: a command and what it does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Example {
    pub command: String,
    pub description: String,
}

/// The entries of [`EXAMPLES`], so the manpage and the reference can lay them out
/// their own way. A command is indented two spaces; its description, on the lines
/// after it, six.
pub fn examples() -> Vec<Example> {
    let mut out: Vec<Example> = Vec::new();
    for line in EXAMPLES.lines() {
        if let Some(text) = line.strip_prefix("      ") {
            if let Some(last) = out.last_mut() {
                if !last.description.is_empty() {
                    last.description.push(' ');
                }
                last.description.push_str(text.trim());
            }
        } else if let Some(command) = line.strip_prefix("  ") {
            out.push(Example {
                command: command.trim().to_string(),
                description: String::new(),
            });
        }
    }
    out
}

/// Command-line arguments for datui
#[derive(Clone, Parser, Debug)]
#[command(
    name = "datui",
    version,
    about = "Terminal UI for tabular data",
    long_about = include_str!("../long_about.txt"),
    after_help = EXAMPLES
)]
pub struct Args {
    /// Path(s) to the data file(s) to open.
    /// Multiple files of the same format are concatenated into one table.
    /// `-` reads data piped to standard input, as does no PATH when something is piped in.
    /// With no PATH and nothing piped in, datui opens its home screen so you can pick a dataset.
    #[arg(num_args = 0.., value_name = "PATH")]
    pub paths: Vec<std::path::PathBuf>,

    /// Skip this many raw lines at the start of the file, split on newlines alone. Not quote-aware: a newline inside a quoted field counts. Compare --skip-rows
    #[arg(long = "skip-lines", value_name = "N", help_heading = "Reading")]
    pub skip_lines: Option<usize>,

    /// Skip this many CSV rows at the start of the file; the header is read after them. Quote-aware: a row with embedded newlines counts once. Compare --skip-lines
    #[arg(long = "skip-rows", value_name = "N", help_heading = "Reading")]
    pub skip_rows: Option<usize>,

    /// Skip this many rows at the end of the file, such as a vendor footer or trailing garbage. Needs the row count first, which reads the whole file; on a directory in a bucket, every file
    #[arg(long = "skip-tail-rows", value_name = "N", help_heading = "Reading")]
    pub skip_tail_rows: Option<usize>,

    /// Read the first row as data, not column names; columns are named column_1, column_2, …
    #[arg(long = "no-header", value_name = "BOOL", num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = clap::value_parser!(bool), help_heading = "CSV and delimited text")]
    pub no_header: Option<bool>,

    /// Column separator for a delimited text file, as an ASCII code (9 for tab). Default: `,` for .csv, tab for .tsv, `|` for .psv
    #[arg(
        long = "delimiter",
        value_name = "CODE",
        help_heading = "CSV and delimited text"
    )]
    pub delimiter: Option<u8>,

    /// Number of rows to use when inferring CSV schema (default: 1000). Larger values reduce the risk of a wrong type (e.g. int then N/A)
    #[arg(
        long = "infer-schema-length",
        value_name = "N",
        help_heading = "CSV and delimited text"
    )]
    pub infer_schema_length: Option<usize>,

    /// When reading CSV, ignore parse errors and continue with the next batch (default: false)
    #[arg(long = "ignore-errors", value_name = "BOOL", num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = clap::value_parser!(bool), help_heading = "CSV and delimited text")]
    pub ignore_errors: Option<bool>,

    /// Treat these values as null when reading CSV. Use once per value; no "=" means all columns, COL=VAL means column COL only (first "=" separates column from value). Example: --null-value NA --null-value amount=
    #[arg(
        long = "null-value",
        value_name = "VAL",
        help_heading = "CSV and delimited text"
    )]
    pub null_value: Vec<String>,

    /// Compression format, when the extension does not say (default: auto-detected from the extension)
    #[arg(long = "compression", value_enum, help_heading = "Reading")]
    pub compression: Option<CompressionFormat>,

    /// File format, for a URL or a path whose extension does not say (default: auto-detected from the extension)
    #[arg(long = "format", value_enum, help_heading = "Reading")]
    pub format: Option<FileFormat>,

    /// Enable debug mode to show operational information
    #[arg(long = "debug", action)]
    pub debug: bool,

    /// Write the log here (default: [debug] log_file, or datui.log in the cache directory). DATUI_LOG sets the level: error, warn (default), info, debug or off
    #[arg(long = "log-file", value_name = "PATH")]
    pub log_file: Option<std::path::PathBuf>,

    /// Read this as one partitioned table. Not needed for a directory, which datui reads the way Enter reads its row; use it for a glob, or to force partition columns on a layout that does not say so itself. Ignored for a single file
    #[arg(long = "hive", action, help_heading = "Reading")]
    pub hive: bool,

    /// Combine Parquet file schemas from their footers (default: true). Set to false to use Polars' single-file schema inference
    #[arg(long = "single-spine-schema", value_name = "BOOL", num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = clap::value_parser!(bool), help_heading = "Reading")]
    pub single_spine_schema: Option<bool>,

    /// Parse CSV and JSON string columns that look like dates or ISO 8601 timestamps (e.g. 2024-01-31, 2024-01-31T10:00:00Z) as Date or Datetime (default: true)
    #[arg(long = "parse-dates", value_name = "BOOL", num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = clap::value_parser!(bool), help_heading = "CSV and delimited text")]
    pub parse_dates: Option<bool>,

    /// Trim whitespace and parse CSV string columns as date, datetime, time, duration, int, or float (default: all string columns). --parse-strings=COL (repeatable) limits it to named columns; --no-parse-strings disables it
    #[arg(long = "parse-strings", value_name = "COL", num_args = 0.., require_equals = true, default_missing_value = "", help_heading = "CSV and delimited text")]
    pub parse_strings: Vec<String>,

    /// Do not trim or type-infer CSV string columns. Overrides config and --parse-strings
    #[arg(
        long = "no-parse-strings",
        action,
        help_heading = "CSV and delimited text"
    )]
    pub no_parse_strings: bool,

    /// Decompress into memory (default: decompress to a temp file and scan lazily)
    #[arg(long = "decompress-in-memory", value_name = "BOOL", require_equals = true, default_missing_value = "true", num_args = 0..=1, value_parser = clap::value_parser!(bool), help_heading = "Reading")]
    pub decompress_in_memory: Option<bool>,

    /// Directory for decompression temp files (default: system temp, e.g. TMPDIR)
    #[arg(long = "temp-dir", value_name = "DIR", help_heading = "Reading")]
    pub temp_dir: Option<std::path::PathBuf>,

    /// Excel sheet to load: 0-based index (e.g. 0) or sheet name (e.g. "Sales")
    #[arg(long = "sheet", value_name = "SHEET", help_heading = "Reading")]
    pub excel_sheet: Option<String>,

    /// Forget every recently opened dataset and exit; other caches are kept
    #[arg(long = "clear-recents", action, help_heading = "Maintenance")]
    pub clear_recents: bool,

    /// Clear all cache data and exit
    #[arg(long = "clear-cache", action, help_heading = "Maintenance")]
    pub clear_cache: bool,

    /// Apply a saved view by name when starting the application
    #[arg(long = "template", value_name = "NAME")]
    pub template: Option<String>,

    /// Remove all saved views and exit
    #[arg(long = "remove-templates", action, help_heading = "Maintenance")]
    pub remove_templates: bool,

    /// Rows an analysis samples from a larger table, spread across all of it (default: [performance] analysis_sample_rows, 100000). 0 reads every row
    #[arg(long = "sample-rows", value_name = "N", help_heading = "Performance")]
    pub sample_rows: Option<usize>,

    /// Use the Polars streaming engine where available (default: true)
    #[arg(long = "polars-streaming", value_name = "BOOL", num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = clap::value_parser!(bool), help_heading = "Performance")]
    pub polars_streaming: Option<bool>,

    /// No effect since Polars 0.55: the pivot crash with a Date/Datetime index it worked around is gone. Kept so existing invocations still parse
    #[arg(long = "workaround-pivot-date-index", value_name = "BOOL", value_parser = clap::value_parser!(bool), hide = true)]
    pub workaround_pivot_date_index: Option<bool>,

    /// Pages to buffer ahead of the visible area (default: 3). More is smoother scrolling, more memory
    #[arg(
        long = "pages-lookahead",
        value_name = "N",
        help_heading = "Performance"
    )]
    pub pages_lookahead: Option<usize>,

    /// Pages to buffer behind the visible area (default: 3). More is smoother scrolling, more memory
    #[arg(
        long = "pages-lookback",
        value_name = "N",
        help_heading = "Performance"
    )]
    pub pages_lookback: Option<usize>,

    /// Show row numbers on the left side of the table. Press N to toggle while running
    #[arg(long = "row-numbers", action, help_heading = "Display")]
    pub row_numbers: bool,

    /// Starting index for row numbers (default: 1)
    #[arg(long = "row-start-index", value_name = "N", help_heading = "Display")]
    pub row_start_index: Option<usize>,

    /// Color table cells by column type (default: true)
    #[arg(long = "column-colors", value_name = "BOOL", num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = clap::value_parser!(bool), help_heading = "Display")]
    pub column_colors: Option<bool>,

    /// Digit grouping for numbers in the table (default: none). "system" reads LC_ALL/LC_NUMERIC/LANG. Press , to toggle while running
    #[arg(long = "number-format", value_name = "FORMAT", value_parser = clap::builder::PossibleValuesParser::new(NUMBER_FORMAT_VALUES), help_heading = "Display")]
    pub number_format: Option<String>,

    /// Right-align numeric columns and their headers (default: true)
    #[arg(long = "align-numeric-right", value_name = "BOOL", num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = clap::value_parser!(bool), help_heading = "Display")]
    pub align_numeric_right: Option<bool>,

    /// Write the default configuration to ~/.config/datui/config.toml and exit
    #[arg(long = "generate-config", action, help_heading = "Maintenance")]
    pub generate_config: bool,

    /// Overwrite an existing config file (with --generate-config)
    #[arg(
        long = "force",
        requires = "generate_config",
        action,
        help_heading = "Maintenance"
    )]
    pub force: bool,

    /// S3-compatible endpoint URL (overrides config and AWS_ENDPOINT_URL). Example: http://localhost:9000
    #[arg(long = "s3-endpoint-url", value_name = "URL", help_heading = "Cloud")]
    pub s3_endpoint_url: Option<String>,

    /// S3 access key (overrides config and AWS_ACCESS_KEY_ID)
    #[arg(long = "s3-access-key-id", value_name = "KEY", help_heading = "Cloud")]
    pub s3_access_key_id: Option<String>,

    /// S3 secret key (overrides config and AWS_SECRET_ACCESS_KEY)
    #[arg(
        long = "s3-secret-access-key",
        value_name = "SECRET",
        help_heading = "Cloud"
    )]
    pub s3_secret_access_key: Option<String>,

    /// S3 region (overrides config and AWS_REGION). Example: us-east-1
    #[arg(long = "s3-region", value_name = "REGION", help_heading = "Cloud")]
    pub s3_region: Option<String>,

    /// Which cloud logins found on this machine appear on the home screen: all, none, or kinds separated by commas (s3, gcs, azure). Overrides [cloud] discover. Entries in [[cloud.connections]] always appear
    #[arg(long = "cloud-discover", value_name = "WHICH", value_parser = parse_cloud_discover, help_heading = "Cloud")]
    pub cloud_discover: Option<String>,
}

/// `all`, `none`, or kinds from `s3`, `gcs` and `azure` separated by commas. The same
/// words `[cloud] discover` takes, checked here so a typo stops at the command line.
fn parse_cloud_discover(text: &str) -> Result<String, String> {
    const KINDS: [&str; 3] = ["s3", "gcs", "azure"];
    let text = text.trim().to_ascii_lowercase();
    if text == "all" || text == "none" {
        return Ok(text);
    }
    for kind in text.split(',') {
        if !KINDS.contains(&kind.trim()) {
            return Err(format!(
                "\"{}\" is not all, none, or a kind: {}",
                kind.trim(),
                KINDS.join(", ")
            ));
        }
    }
    Ok(text)
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
        if id == "help" || id == "version" || arg.is_hide_set() {
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
            } else if arg.get_num_args().is_some_and(|n| n.min_values() == 0) {
                // The value is optional and, where one is given, spelled with `=`.
                format!("{op}[={placeholder}]")
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

    out.push_str("\n## Examples\n\n| Command | Does |\n|---------|------|\n");
    for example in examples() {
        out.push_str(&format!(
            "| `{}` | {} |\n",
            escape_table_cell(&example.command),
            escape_table_cell(&example.description)
        ));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The manpage and the reference read `examples.txt` through `examples()`, so a
    /// line indented the wrong way would drop or merge an entry there while `--help`
    /// still looked right.
    #[test]
    fn every_example_has_a_command_and_a_description() {
        let examples = examples();
        assert!(examples.len() >= 4);
        for example in &examples {
            // A pipe into datui is a command too.
            assert!(
                example.command.starts_with("datui") || example.command.contains("| datui"),
                "{example:?}"
            );
            assert!(!example.description.is_empty(), "{example:?}");
        }
        let listed = EXAMPLES
            .lines()
            .filter(|l| l.starts_with("  ") && !l.starts_with("   "))
            .count();
        assert_eq!(listed, examples.len());
    }

    /// `-` is a path like any other to the parser: standard input, with the reading
    /// flags beside it.
    #[test]
    fn a_dash_names_standard_input() {
        let args = Args::try_parse_from(["datui", "-", "--format", "jsonl"]).unwrap();
        assert_eq!(args.paths, vec![std::path::PathBuf::from("-")]);
        assert_eq!(args.format, Some(FileFormat::Jsonl));
        let args = Args::try_parse_from(["datui", "--delimiter", "59", "-"]).unwrap();
        assert_eq!(args.paths, vec![std::path::PathBuf::from("-")]);
        assert_eq!(args.delimiter, Some(b';'));
        assert!(EXAMPLES.contains("| datui"), "the help shows a pipe");
    }

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
        assert_eq!(
            FileFormat::from_path(Path::new("model.gguf")),
            Some(FileFormat::Gguf)
        );
        assert_eq!(
            FileFormat::from_path(Path::new("model-00001-of-00002.safetensors")),
            Some(FileFormat::Safetensors)
        );
        assert_eq!(
            FileFormat::from_path(Path::new("model.safetensors.index.json")),
            Some(FileFormat::Safetensors),
            "an index is read as the shards it names"
        );
        assert_eq!(
            FileFormat::from_path(Path::new("config.json")),
            Some(FileFormat::Json)
        );
    }
}

#[cfg(test)]
mod format_tests {
    use super::FileFormat;

    /// `ALL` is the list `from_name` searches, so a format missing from it cannot be
    /// read back from a stored name. The match below is exhaustive, so a new variant
    /// does not compile until it is written here — the reminder to add it to `ALL`,
    /// which nothing can enforce outright.
    #[test]
    fn every_format_is_listed() {
        fn listed(f: FileFormat) -> bool {
            match f {
                FileFormat::Parquet
                | FileFormat::Csv
                | FileFormat::Tsv
                | FileFormat::Psv
                | FileFormat::Json
                | FileFormat::Jsonl
                | FileFormat::Arrow
                | FileFormat::Avro
                | FileFormat::Orc
                | FileFormat::Excel
                | FileFormat::Safetensors
                | FileFormat::Gguf => FileFormat::ALL.contains(&f),
            }
        }
        for format in FileFormat::ALL {
            assert!(listed(format), "{format:?}");
            assert_eq!(FileFormat::from_name(format.name()), Some(format));
        }

        // And the names themselves, because they are not only labels. `Holds` keeps a
        // format as this string and the dataset cache writes it out, so renaming one
        // changes what every directory of that format reads *and* makes the records
        // already on disk unreadable — `from_name` then answers `None`, which the
        // enrich gate takes for "not Parquet" and blanks the directory's size. The round
        // trip above holds for any string; this is what says which.
        assert_eq!(
            FileFormat::ALL.map(|f| f.name()),
            [
                "parquet",
                "csv",
                "tsv",
                "psv",
                "json",
                "jsonl",
                "arrow",
                "avro",
                "orc",
                "excel",
                "safetensors",
                "gguf"
            ]
        );
    }
}
