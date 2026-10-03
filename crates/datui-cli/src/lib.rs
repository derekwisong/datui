//! Shared CLI definitions for datui.
//!
//! Used by the main application and by the build script (manpage) and
//! gen_docs binary (command-line-options markdown).

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use std::path::Path;

mod formats;
pub use formats::*;

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

    /// Follow a file as it grows, as tail -f does: rows appended to a local CSV, TSV, PSV or NDJSON file, or still arriving on standard input (-), show as they land. t pauses and resumes; Esc stops
    #[arg(short = 'f', long = "follow", action, help_heading = "Reading")]
    pub follow: bool,

    /// Record standard input to FILE while viewing it: the bytes exactly as they arrive, in any format. Never replaces FILE without --force. A WAV file's sizes are filled in when the stream ends
    #[arg(long = "tee", value_name = "FILE", help_heading = "Reading")]
    pub tee: Option<std::path::PathBuf>,

    /// With --tee: leave FILE exactly as the bytes came, a WAV header's sizes included
    #[arg(long = "tee-raw", requires = "tee", action, help_heading = "Reading")]
    pub tee_raw: bool,

    /// Skip this many raw lines at the start of the file, split on newlines alone. Not quote-aware: a newline inside a quoted field counts. Compare --skip-rows
    #[arg(long = "skip-lines", value_name = "N", help_heading = "Reading")]
    pub skip_lines: Option<usize>,

    /// Skip this many CSV rows at the start of the file; the header is read after them. Quote-aware: a row with embedded newlines counts once. Compare --skip-lines
    #[arg(long = "skip-rows", value_name = "N", help_heading = "Reading")]
    pub skip_rows: Option<usize>,

    /// Skip this many rows at the end of the file, such as a vendor footer or trailing garbage. Needs the row count first, which reads the whole file; on a directory in a bucket, every file
    #[arg(long = "skip-tail-rows", value_name = "N", help_heading = "Reading")]
    pub skip_tail_rows: Option<usize>,

    /// Lines that start with this are comments and are skipped, before the header and among the data; the header is the first line that is not one. Frictionless `commentChar`
    #[arg(
        long = "comment-char",
        value_name = "C",
        value_parser = parse_comment_char,
        help_heading = "CSV and delimited text"
    )]
    pub comment_char: Option<String>,

    /// The line, or comma-separated lines, that hold the header, counted from 1 at the top of the file before anything is skipped. Several are joined per column with [file_loading] header_join (default a space); the data starts after the last. Frictionless `headerRows`
    #[arg(
        long = "header-rows",
        value_name = "N[,M...]",
        value_delimiter = ',',
        value_parser = clap::value_parser!(u64).range(1..),
        help_heading = "CSV and delimited text"
    )]
    pub header_rows: Vec<u64>,

    /// Ignore the spaces after a delimiter, so padded numbers read as numbers and a cell of spaces is null. Frictionless `skipInitialSpace`
    #[arg(long = "skip-initial-space", value_name = "BOOL", num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = clap::value_parser!(bool), help_heading = "CSV and delimited text")]
    pub skip_initial_space: Option<bool>,

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

    #[arg(long = "format", value_name = "FORMAT", value_parser = parse_format, help = format_help(), help_heading = "Reading")]
    pub format: Option<FormatChoice>,

    /// Read the file (or directory of column files) through this binary format spec, whatever else matches it. FILE may be an http(s), s3, gs or az URL; a spec is at most 1 MiB
    #[arg(long = "spec", value_name = "FILE", help_heading = "Reading")]
    pub spec: Option<std::path::PathBuf>,

    /// Decode a candump log's frames with this DBC file too, over those on the format search path: a .dbc file, or TOML with kind = "dbc"
    #[arg(long = "dbc", value_name = "FILE", help_heading = "Reading")]
    pub dbc: Option<std::path::PathBuf>,

    /// Read a FIX log with this dictionary too, over the built-in one and those on the format search path: a QuickFIX XML data dictionary, or TOML with kind = "fix"
    #[arg(long = "fix-dict", value_name = "FILE", help_heading = "Reading")]
    pub fix_dict: Option<std::path::PathBuf>,
    /// Read one variant of a binary format spec's records alone, as its own table: only its records, and only its columns
    #[arg(long = "variant", value_name = "NAME", help_heading = "Reading")]
    pub variant: Option<String>,
    /// Show the file's bytes in the hex view, whatever it holds. A local file no reader and no spec takes opens there anyway
    #[arg(long = "hex", action, help_heading = "Reading")]
    pub hex: bool,

    /// Bytes a row of the hex view holds, so that records line up (default: 8, 16, 32 or 64, as many as fit)
    #[arg(long = "record-size", value_name = "N", value_parser = clap::value_parser!(u16).range(1..=4096), help_heading = "Reading")]
    pub record_size: Option<u16>,

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

    #[arg(long = "table", value_name = "TABLE", help = table_help(), help_heading = "Reading")]
    pub table: Option<String>,
    /// Show integer audio samples as float in [-1, 1] (default: the integers as stored)
    #[arg(long = "normalize", help_heading = "Reading")]
    pub normalize: bool,

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

    /// Take the mouse: wheel scrolls, click selects (default: true). --mouse=false leaves it to the terminal
    #[arg(long = "mouse", value_name = "BOOL", num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = clap::value_parser!(bool), help_heading = "Display")]
    pub mouse: Option<bool>,

    /// Write the default configuration to ~/.config/datui/config.toml and exit
    #[arg(long = "generate-config", action, help_heading = "Maintenance")]
    pub generate_config: bool,

    /// Overwrite an existing file: the config file with --generate-config, or FILE with --tee
    #[arg(long = "force", action, help_heading = "Maintenance")]
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

    #[command(subcommand)]
    pub command: Option<Command>,
}

/// What `--format` names: a format datui reads, or a binary format spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatChoice {
    Builtin(FileFormat),
    /// A spec on the search path, by its namespaced name.
    Spec(String),
}

impl FormatChoice {
    /// The built-in format, when that is what was named.
    pub fn builtin(&self) -> Option<FileFormat> {
        match self {
            Self::Builtin(format) => Some(*format),
            Self::Spec(_) => None,
        }
    }

    /// How a file read this way is read when opened: a built-in format's
    /// [`FileFormat::read_mode`], or a spec's, whose records are decoded from a map of
    /// the file (or of its decompressed copy) only where they are shown.
    pub fn read_mode(&self, stored: Stored) -> Option<ReadMode> {
        match self {
            Self::Builtin(format) => format.read_mode(stored),
            Self::Spec(_) => match stored {
                Stored::Plain => Some(ReadMode::Lazy),
                Stored::Compressed { .. } => Some(ReadMode::Converted),
                Stored::Stream => None,
            },
        }
    }

    /// How one object read this way in a bucket is read. A spec reads local files, so
    /// a spec's object is downloaded first.
    pub fn bucket_object(&self, stored: Stored) -> RemoteRead {
        match self {
            Self::Builtin(format) => format.bucket_object(stored),
            Self::Spec(_) => RemoteRead::Downloaded,
        }
    }

    /// How one HTTP(S) file read this way is read; a spec's is downloaded first.
    pub fn http_file(&self) -> RemoteRead {
        match self {
            Self::Builtin(format) => format.http_file(),
            Self::Spec(_) => RemoteRead::Downloaded,
        }
    }

    /// How a bucket prefix read this way is read as one table; a spec's is not.
    pub fn bucket_prefix(&self, stored: Stored) -> Option<RemoteRead> {
        match self {
            Self::Builtin(format) => format.bucket_prefix(stored),
            Self::Spec(_) => None,
        }
    }

    /// The spec's name, when a spec was named.
    pub fn spec(&self) -> Option<&str> {
        match self {
            Self::Builtin(_) => None,
            Self::Spec(name) => Some(name),
        }
    }
}

/// A built-in format's name, or a spec's: namespaced (`acme.l2feed`), so one can never
/// be taken for the other.
fn parse_format(text: &str) -> Result<FormatChoice, String> {
    if let Some(format) = FileFormat::from_name(&text.to_ascii_lowercase()) {
        return Ok(FormatChoice::Builtin(format));
    }
    let spec_like = text.contains('.')
        && !text.starts_with('.')
        && !text.ends_with('.')
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if spec_like {
        return Ok(FormatChoice::Spec(text.to_string()));
    }
    let names: Vec<&str> = FileFormat::ALL.iter().map(|f| f.name()).collect();
    Err(format!(
        "\"{text}\" is not a format: {}, or a spec name such as acme.l2feed (`datui formats` lists them)",
        names.join(", ")
    ))
}

/// Commands besides opening data.
#[derive(Clone, Debug, Subcommand)]
pub enum Command {
    /// List the binary format specs and FIX dictionaries on the search path: each one's name, what it matches, the file it came from, and the copies it overrides
    Formats {
        #[command(subcommand)]
        action: Option<FormatsAction>,
    },
}

/// What `datui formats` does besides listing.
#[derive(Clone, Debug, Subcommand)]
pub enum FormatsAction {
    /// Check a spec or FIX dictionary, by name or by file; with FILE, print its first decoded rows. Exits non-zero on an error
    Check {
        /// A spec or FIX dictionary name on the search path, or its file
        #[arg(value_name = "SPEC")]
        spec: String,
        /// A file (or directory of column files) to read with it
        #[arg(value_name = "FILE")]
        file: Option<std::path::PathBuf>,
    },
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

/// Why `c` cannot mark comment lines, if it cannot: it must be something, and on one
/// line. `--comment-char`, `[file_loading] comment_char` and the Python option share it.
pub fn check_comment_char(c: &str) -> Result<(), String> {
    if c.is_empty() {
        return Err("must not be empty".into());
    }
    if c.contains(['\n', '\r']) {
        return Err("must not contain a line break".into());
    }
    Ok(())
}

fn parse_comment_char(text: &str) -> Result<String, String> {
    check_comment_char(text).map(|()| text.to_string())
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

    out.push_str("\n## Commands\n\n| Command | Does |\n|---------|------|\n");
    for sub in cmd.get_subcommands().filter(|c| c.get_name() != "help") {
        let about = |c: &clap::Command| {
            c.get_about()
                .map(|a| escape_table_cell(&a.to_string()))
                .unwrap_or_default()
        };
        out.push_str(&format!(
            "| `datui {}` | {} |\n",
            sub.get_name(),
            about(sub)
        ));
        for action in sub.get_subcommands().filter(|c| c.get_name() != "help") {
            let operands: Vec<String> = action
                .get_arguments()
                .filter(|a| a.is_positional())
                .filter_map(|a| {
                    let name = a.get_value_names()?.first()?.to_string();
                    Some(if a.is_required_set() {
                        name
                    } else {
                        format!("[{name}]")
                    })
                })
                .collect();
            out.push_str(&format!(
                "| `datui {} {} {}` | {} |\n",
                sub.get_name(),
                action.get_name(),
                operands.join(" "),
                about(action)
            ));
        }
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
        assert_eq!(args.format, Some(FormatChoice::Builtin(FileFormat::Jsonl)));
        let args = Args::try_parse_from(["datui", "--delimiter", "59", "-"]).unwrap();
        assert_eq!(args.paths, vec![std::path::PathBuf::from("-")]);
        assert_eq!(args.delimiter, Some(b';'));
        assert!(EXAMPLES.contains("| datui"), "the help shows a pipe");
    }

    /// `--format` takes a built-in format or a spec's namespaced name, and nothing else.
    #[test]
    fn a_format_is_built_in_or_a_spec_name() {
        let args = Args::try_parse_from(["datui", "x.l2", "--format", "acme.l2feed"]).unwrap();
        assert_eq!(args.format, Some(FormatChoice::Spec("acme.l2feed".into())));
        let args = Args::try_parse_from(["datui", "x", "--format", "CSV"]).unwrap();
        assert_eq!(args.format, Some(FormatChoice::Builtin(FileFormat::Csv)));
        let refused = Args::try_parse_from(["datui", "x", "--format", "cvs"]).unwrap_err();
        assert!(refused.to_string().contains("spec name"), "{refused}");
    }

    /// `datui formats` is a command; any other first word is still a path.
    #[test]
    fn formats_is_a_command_and_paths_stay_paths() {
        let args = Args::try_parse_from(["datui", "formats"]).unwrap();
        assert!(matches!(
            args.command,
            Some(Command::Formats { action: None })
        ));
        let args = Args::try_parse_from(["datui", "formats", "check", "a.b", "f.bin"]).unwrap();
        let Some(Command::Formats {
            action: Some(FormatsAction::Check { spec, file }),
        }) = args.command
        else {
            panic!("a check");
        };
        assert_eq!((spec.as_str(), file), ("a.b", Some("f.bin".into())));
        let args = Args::try_parse_from(["datui", "data.csv", "--spec", "s.toml"]).unwrap();
        assert_eq!(args.paths, vec![std::path::PathBuf::from("data.csv")]);
        assert!(args.command.is_none());
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
            FileFormat::from_path(Path::new("song.MID")),
            Some(FileFormat::Midi)
        );
        assert_eq!(
            FileFormat::from_path(Path::new("karaoke.kar")),
            Some(FileFormat::Midi)
        );
        assert_eq!(
            FileFormat::from_path(Path::new("dump.vcd")),
            Some(FileFormat::Vcd)
        );
        for name in ["lib.sdf", "lib.SD"] {
            assert_eq!(
                FileFormat::from_path(Path::new(name)),
                Some(FileFormat::Sdf),
                "{name}"
            );
        }
        assert_eq!(
            FileFormat::from_path(Path::new("config.json")),
            Some(FileFormat::Json)
        );
        for name in [
            "take.wav",
            "take.BWF",
            "mix.rf64",
            "loop.aif",
            "loop.aiff",
            "x.aifc",
        ] {
            assert_eq!(
                FileFormat::from_path(Path::new(name)),
                Some(FileFormat::Audio),
                "{name}"
            );
        }
    }
}

#[cfg(test)]
mod format_tests {
    use super::{FileFormat, FormatChoice, ReadMode, RemoteRead, Stored};

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
                | FileFormat::Gguf
                | FileFormat::Nmea
                | FileFormat::Gpx
                | FileFormat::Audio
                | FileFormat::Midi
                | FileFormat::Sqlite
                | FileFormat::Vcd
                | FileFormat::Fix
                | FileFormat::Sdf
                | FileFormat::Numpy
                | FileFormat::Elf
                | FileFormat::Ulog
                | FileFormat::Dataflash
                | FileFormat::Candump => FileFormat::ALL.contains(&f),
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
                "gguf",
                "nmea",
                "gpx",
                "audio",
                "midi",
                "sqlite",
                "vcd",
                "fix",
                "sdf",
                "numpy",
                "elf",
                "ulog",
                "dataflash",
                "candump"
            ]
        );
    }

    /// Each way a file can sit on disk reads as the format says, and the formats that
    /// take the whole file into memory are the ones whose readers do.
    #[test]
    fn read_mode_by_format_and_storage() {
        use ReadMode::*;
        let plain = |f: FileFormat| f.read_mode(Stored::Plain);
        assert_eq!(plain(FileFormat::Parquet), Some(Lazy));
        assert_eq!(plain(FileFormat::Csv), Some(Lazy));
        assert_eq!(plain(FileFormat::Arrow), Some(Lazy));
        assert_eq!(plain(FileFormat::Audio), Some(Lazy));
        assert_eq!(plain(FileFormat::Sqlite), Some(Lazy));
        assert_eq!(plain(FileFormat::Nmea), Some(Converted));
        assert_eq!(plain(FileFormat::Gpx), Some(Converted));
        for f in [
            FileFormat::Json,
            FileFormat::Jsonl,
            FileFormat::Avro,
            FileFormat::Orc,
            FileFormat::Excel,
            FileFormat::Safetensors,
            FileFormat::Gguf,
            FileFormat::Midi,
        ] {
            assert_eq!(plain(f), Some(InMemory), "{}", f.name());
        }
        for f in FileFormat::ALL {
            assert!(plain(f).is_some(), "every format opens: {}", f.name());
        }

        // An IPC stream is converted; nothing else is one.
        assert_eq!(FileFormat::Arrow.read_mode(Stored::Stream), Some(Converted));
        assert_eq!(FileFormat::Parquet.read_mode(Stored::Stream), None);

        // Compressed text is decompressed once to a file, or read in memory when asked;
        // a GPS log is decompressed as it is converted; nothing else opens compressed.
        let compressed = |f: FileFormat, in_memory| f.read_mode(Stored::Compressed { in_memory });
        for f in [FileFormat::Csv, FileFormat::Tsv, FileFormat::Psv] {
            assert_eq!(compressed(f, false), Some(Converted));
            assert_eq!(compressed(f, true), Some(InMemory));
        }
        assert_eq!(compressed(FileFormat::Nmea, true), Some(Converted));
        assert_eq!(compressed(FileFormat::Parquet, false), None);
        assert_eq!(compressed(FileFormat::Json, false), None);
        assert_eq!(compressed(FileFormat::Sqlite, false), None);

        // A spec maps its file, or the decompressed copy of it.
        let spec = FormatChoice::Spec("acme.l2feed".into());
        assert_eq!(spec.read_mode(Stored::Plain), Some(Lazy));
        assert_eq!(
            spec.read_mode(Stored::Compressed { in_memory: true }),
            Some(Converted)
        );
        assert_eq!(spec.bucket_object(Stored::Plain), RemoteRead::Downloaded);

        // Parquet objects, Arrow IPC files and model headers are read in place; CSV
        // and NDJSON prefixes are too. Over HTTP only a model's header is.
        let model = |f: FileFormat| matches!(f, FileFormat::Safetensors | FileFormat::Gguf);
        for f in FileFormat::ALL {
            let in_place = f.bucket_object(Stored::Plain) == RemoteRead::InPlace;
            assert_eq!(
                in_place,
                matches!(f, FileFormat::Parquet | FileFormat::Arrow) || model(f),
                "{}",
                f.name()
            );
            assert_eq!(
                f.bucket_object(Stored::Compressed { in_memory: false }),
                RemoteRead::Downloaded
            );
            assert_eq!(
                f.http_file() == RemoteRead::InPlace,
                model(f),
                "{}",
                f.name()
            );
        }
        assert_eq!(spec.http_file(), RemoteRead::Downloaded);
        let prefixes: Vec<_> = FileFormat::ALL
            .into_iter()
            .filter(|f| f.reads_bucket_prefix())
            .collect();
        assert_eq!(
            prefixes,
            [
                FileFormat::Parquet,
                FileFormat::Csv,
                FileFormat::Jsonl,
                FileFormat::Arrow,
                FileFormat::Safetensors,
                FileFormat::Gguf
            ]
        );
        // Streams have no footer: one object, or a prefix of them, is downloaded.
        assert_eq!(
            FileFormat::Arrow.bucket_object(Stored::Stream),
            RemoteRead::Downloaded
        );
        assert_eq!(
            FileFormat::Arrow.bucket_prefix(Stored::Stream),
            Some(RemoteRead::Downloaded)
        );
    }

    /// The loading-data page's format table says what `read_mode` and the remote
    /// methods say, for every format, and names every format. A row is matched to its
    /// formats by the extensions it lists, so the extensions are checked too.
    #[test]
    fn the_docs_format_table_agrees_with_the_code() {
        let page = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/user-guide/loading-data.md");
        let text = std::fs::read_to_string(&page).expect("the loading-data page");
        let header =
            "| Format | Extensions | Read | Compressed | HTTP(S) | In a bucket | Bucket prefix |";
        let start = text.find(header).expect("the format table");
        let rows: Vec<Vec<String>> = text[start..]
            .lines()
            .skip(2)
            .take_while(|l| l.starts_with('|'))
            .map(|l| {
                l.trim_matches('|')
                    .split(" | ")
                    .map(|c| c.trim().to_string())
                    .collect()
            })
            .collect();
        let said = |mode: Option<ReadMode>| mode.map_or("no", ReadMode::label);
        let mut seen: Vec<FileFormat> = Vec::new();
        let (mut stream_row, mut spec_row) = (false, false);
        for row in &rows {
            let [format, extensions, read, compressed, http, bucket, prefix] = &row[..] else {
                panic!("seven cells: {row:?}");
            };
            let choices: Vec<FormatChoice> = if extensions.contains("format spec") {
                spec_row = true;
                vec![FormatChoice::Spec("any.spec".into())]
            } else if let Some((_, name)) = extensions.split_once("--format ") {
                // A format found by content, with no extension: FIX.
                let name = name.trim_matches('`');
                vec![FormatChoice::Builtin(
                    FileFormat::from_name(name).unwrap_or_else(|| panic!("{name} is a format")),
                )]
            } else {
                extensions
                    .split(", ")
                    .map(|e| {
                        let name = e.trim_matches('`');
                        let found = match name.strip_prefix('.') {
                            Some(ext) => FileFormat::from_extension(ext),
                            None => FileFormat::from_path(std::path::Path::new(name)),
                        };
                        FormatChoice::Builtin(found.unwrap_or_else(|| panic!("{name} is read")))
                    })
                    .collect()
            };
            let stored = if format.contains("stream") {
                stream_row = true;
                Stored::Stream
            } else {
                Stored::Plain
            };
            for choice in choices {
                if let (FormatChoice::Builtin(f), Stored::Plain) = (&choice, stored) {
                    seen.push(*f);
                }
                assert_eq!(read, said(choice.read_mode(stored)), "{format}: Read");
                assert_eq!(
                    compressed,
                    said(choice.read_mode(Stored::Compressed { in_memory: false })),
                    "{format}: Compressed"
                );
                assert_eq!(http, choice.http_file().label(), "{format}: HTTP(S)");
                assert_eq!(
                    bucket,
                    choice.bucket_object(stored).label(),
                    "{format}: In a bucket"
                );
                let as_prefix = choice.bucket_prefix(stored).map_or("no", RemoteRead::label);
                assert_eq!(prefix, as_prefix, "{format}: Bucket prefix");
            }
        }
        for f in FileFormat::ALL {
            assert!(seen.contains(&f), "{} has a row", f.name());
        }
        assert!(stream_row && spec_row, "streams and specs have rows");
    }
}
