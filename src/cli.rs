use clap::{Parser, ValueEnum};
use std::path::PathBuf;

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

/// Command-line arguments for datui
#[derive(Parser, Debug)]
#[command(version, about = "datui")]
pub struct Args {
    pub path: PathBuf,

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
}
