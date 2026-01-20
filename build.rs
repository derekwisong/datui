use clap::{CommandFactory, Parser, ValueEnum};
use clap_mangen::Man;
use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;

/// Compression format for data files
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum CompressionFormat {
    /// Gzip compression (.gz) - Most common, good balance of speed and compression
    Gzip,
    /// Zstandard compression (.zst) - Modern, fast compression with good ratios
    Zstd,
    /// Bzip2 compression (.bz2) - Good compression ratio, slower than gzip
    Bzip2,
    /// XZ compression (.xz) - Excellent compression ratio, slower than bzip2
    Xz,
}

// Duplicate Args struct for build.rs since it can't access the library crate
#[derive(Parser, Debug)]
#[command(version, about = "datui")]
struct Args {
    path: PathBuf,

    /// Skip this many lines when reading a file
    #[arg(long = "skip-lines")]
    skip_lines: Option<usize>,

    /// Skip this many rows when reading a file
    #[arg(long = "skip-rows")]
    skip_rows: Option<usize>,

    /// Specify that the file has no header
    #[arg(long = "no-header")]
    no_header: Option<bool>,

    /// Specify the delimiter to use when reading a file
    #[arg(long = "delimiter")]
    delimiter: Option<u8>,

    /// Specify the compression format explicitly (gzip, zstd, bzip2, xz)
    /// If not specified, compression is auto-detected from file extension.
    /// Supported formats: gzip (.gz), zstd (.zst), bzip2 (.bz2), xz (.xz)
    #[arg(long = "compression", value_enum)]
    compression: Option<CompressionFormat>,

    /// Enable debug mode to show operational information
    #[arg(long = "debug", action)]
    debug: bool,

    /// Clear all cache data and exit
    #[arg(long = "clear-cache", action)]
    clear_cache: bool,

    /// Apply a template by name when starting the application
    #[arg(long = "template")]
    template: Option<String>,

    /// Remove all templates and exit
    #[arg(long = "remove-templates", action)]
    remove_templates: bool,
}

fn main() -> io::Result<()> {
    // Generate manpage using clap_mangen
    let cmd = Args::command();
    let man = Man::new(cmd);
    let mut buffer: Vec<u8> = Default::default();
    man.render(&mut buffer)?;

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Always write to OUT_DIR
    let dest_path = out_dir.join("datui.1");
    fs::write(&dest_path, &buffer)?;

    // In release mode, also write to target/release/ for easy access in CI
    // OUT_DIR is typically target/release/build/xxx/out
    // Going up 3 levels gets us to target/release/
    if env::var("PROFILE").unwrap_or_default() == "release" {
        if let Some(release_dir) = out_dir.ancestors().nth(3) {
            let release_manpage = release_dir.join("datui.1");
            fs::write(&release_manpage, &buffer)?;
        }
    }

    Ok(())
}
