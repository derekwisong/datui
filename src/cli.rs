use clap::Parser;
use std::path::PathBuf;

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
}
