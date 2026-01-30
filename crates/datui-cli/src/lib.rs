//! Shared CLI definitions for datui.
//!
//! Used by the main application and by the build script (manpage) and
//! gen_docs binary (command-line-options markdown).

use clap::{CommandFactory, Parser, ValueEnum};
use std::path::Path;

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
#[derive(Parser, Debug)]
#[command(
    name = "datui",
    version,
    about = "Data Exploration in the Terminal",
    long_about = include_str!("../long_about.txt")
)]
pub struct Args {
    /// Path(s) to the data file(s) to open. Multiple files of the same format are concatenated into one table (not required with --generate-config, --clear-cache, or --remove-templates)
    #[arg(required_unless_present_any = ["generate_config", "clear_cache", "remove_templates"], num_args = 1.., value_name = "PATH")]
    pub paths: Vec<std::path::PathBuf>,

    /// Skip this many lines when reading a file
    #[arg(long = "skip-lines")]
    pub skip_lines: Option<usize>,

    /// Skip this many rows when reading a file
    #[arg(long = "skip-rows")]
    pub skip_rows: Option<usize>,

    /// Specify that the file has no header
    #[arg(long = "no-header")]
    pub no_header: Option<bool>,

    /// Specify the delimiter to use when reading a file
    #[arg(long = "delimiter")]
    pub delimiter: Option<u8>,

    /// Specify the compression format explicitly (gzip, zstd, bzip2, xz)
    /// If not specified, compression is auto-detected from file extension.
    /// Supported formats: gzip (.gz), zstd (.zst), bzip2 (.bz2), xz (.xz)
    #[arg(long = "compression", value_enum)]
    pub compression: Option<CompressionFormat>,

    /// Enable debug mode to show operational information
    #[arg(long = "debug", action)]
    pub debug: bool,

    /// Enable Hive-style partitioning for directory or glob paths; ignored for a single file
    #[arg(long = "hive", action)]
    pub hive: bool,

    /// Try to parse CSV string columns as dates (e.g. YYYY-MM-DD, ISO datetime). Default: true
    #[arg(long = "parse-dates", value_name = "BOOL", value_parser = clap::value_parser!(bool))]
    pub parse_dates: Option<bool>,

    /// Decompress compressed CSV into memory (eager read). Default: decompress to temp file and use lazy scan
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

    /// Generate default configuration file at ~/.config/datui/config.toml
    #[arg(long = "generate-config", action)]
    pub generate_config: bool,

    /// Force overwrite existing config file when using --generate-config
    #[arg(long = "force", requires = "generate_config", action)]
    pub force: bool,
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
}
