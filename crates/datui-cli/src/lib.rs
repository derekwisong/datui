//! Shared CLI definitions for datui.
//!
//! Used by the main application and by the gen_docs binary, which writes the
//! generated docs and the manpages from these definitions.

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use std::path::Path;

pub mod docgen;
pub mod exit;
mod formats;
pub use formats::*;
pub mod keys;
pub mod man;
pub mod settings;
pub mod units;

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

/// The examples: `--help` shows them, and the manpage, the command-line reference and
/// the README render them. One file, so they cannot differ; each runs as written, and
/// `scripts/docs/doc_examples.py` runs them.
pub const EXAMPLES_TOML: &str = include_str!("../examples.toml");

/// How the doc-example runner runs an [`Example`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExampleTest {
    /// Locally, on every pull request.
    Run,
    /// In the nightly job: it reads public data.
    Network,
    /// Locally; a producer that never ends is stopped once the first rows show.
    Interactive,
}

/// One entry of [`EXAMPLES_TOML`]: a command and what it does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Example {
    pub command: String,
    pub description: String,
    pub test: ExampleTest,
    /// What counts as working, when not the default: `rows`, `screen` or `exit`.
    pub expect: Option<String>,
    /// The manpages that show it, as `NAME.SECTION`: `datui.1`, `datui-config.5`.
    pub pages: Vec<String>,
    /// Files the command reads, written before it runs and shown above it.
    pub files: Vec<ExampleFile>,
}

/// A file an [`Example`] reads: its name and its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExampleFile {
    pub name: String,
    pub text: String,
}

/// The entries of [`EXAMPLES_TOML`], in order. Panics on a malformed entry, which
/// `every_example_parses_as_written` and every `--help` reach first.
pub fn examples() -> Vec<Example> {
    let file: toml::Table = EXAMPLES_TOML.parse().expect("examples.toml is TOML");
    let entries = file
        .get("example")
        .and_then(toml::Value::as_array)
        .expect("examples.toml has [[example]] entries");
    entries
        .iter()
        .map(|entry| {
            let table = entry.as_table().expect("an [[example]] is a table");
            for key in table.keys() {
                assert!(
                    matches!(
                        key.as_str(),
                        "command" | "description" | "test" | "expect" | "pages" | "files"
                    ),
                    "examples.toml: unknown key {key}"
                );
            }
            let text = |key: &str| {
                table
                    .get(key)
                    .and_then(toml::Value::as_str)
                    .map(str::to_string)
            };
            let test = match text("test").as_deref() {
                Some("run") => ExampleTest::Run,
                Some("network") => ExampleTest::Network,
                Some("interactive") => ExampleTest::Interactive,
                other => panic!("examples.toml: test = {other:?}"),
            };
            Example {
                command: text("command").expect("an example's command"),
                description: text("description").expect("an example's description"),
                test,
                expect: text("expect"),
                pages: match table.get("pages") {
                    None => vec!["datui.1".to_string()],
                    Some(pages) => pages
                        .as_array()
                        .expect("an example's pages are a list")
                        .iter()
                        .map(|p| p.as_str().expect("a page is NAME.SECTION").to_string())
                        .collect(),
                },
                files: table
                    .get("files")
                    .and_then(toml::Value::as_array)
                    .map(|files| {
                        files
                            .iter()
                            .map(|f| {
                                let text = |key: &str| {
                                    f.get(key)
                                        .and_then(toml::Value::as_str)
                                        .unwrap_or_else(|| panic!("an example's file has a {key}"))
                                        .to_string()
                                };
                                ExampleFile {
                                    name: text("name"),
                                    text: text("text"),
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }
        })
        .collect()
}

/// The examples of manpage `page` (`datui.1`), in order.
pub fn examples_of(page: &str) -> Vec<Example> {
    examples()
        .into_iter()
        .filter(|e| e.pages.iter().any(|p| p == page))
        .collect()
}

/// The examples as `--help` shows them, after the options.
pub fn examples_help() -> String {
    let mut out = String::from("Examples:\n");
    for example in examples_of("datui.1") {
        out.push_str(&format!(
            "  {}\n      {}\n",
            example.command, example.description
        ));
    }
    out.push_str("\nDocs: https://derekwisong.github.io/datui/ and `datui man`\n");
    out.push_str("Keys: datui man keys\n");
    out
}

/// A command's examples, as `datui COMMAND --help` shows them.
fn command_examples(command: &str) -> String {
    let mut out = String::from("Examples:\n");
    for example in examples_of(&format!("datui-{command}.1")) {
        out.push_str(&format!(
            "  {}\n      {}\n",
            example.command, example.description
        ));
    }
    let files: Vec<String> = examples_of(&format!("datui-{command}.1"))
        .into_iter()
        .flat_map(|e| e.files.into_iter().map(|f| f.name))
        .collect();
    if !files.is_empty() {
        out.push_str(&format!(
            "\nThe files they read ({}) are in the manual.",
            files.join(", ")
        ));
    }
    out.push_str(&format!("\nManual: datui man {command}\n"));
    out
}

/// Command-line arguments for datui.
///
/// A flag exists when one invocation needs it: what to open, how to read this file,
/// what to do at start. Everything else is config, set for one run with `-c`. A flag
/// that sets a config key takes its help from the option registry.
#[derive(Clone, Parser, Debug)]
#[command(
    name = "datui",
    version,
    about = "Terminal UI for tabular data",
    long_about = include_str!("../long_about.txt"),
    after_help = examples_help()
)]
pub struct Args {
    /// Files, directories, globs or URLs to open; files of one shape are one table. - reads standard input, as does no PATH when data is piped in. No PATH opens the home screen
    #[arg(num_args = 0.., value_name = "PATH")]
    pub paths: Vec<std::path::PathBuf>,

    #[arg(short = 'F', long = "format", value_name = "FMT", value_parser = parse_format, help = format_help(), help_heading = "Open")]
    pub format: Option<FormatChoice>,

    #[arg(short = 't', long = "table", value_name = "NAME", help = table_help(), help_heading = "Open")]
    pub table: Option<String>,

    /// Read a glob as one partitioned table, or force partition columns on a directory whose layout does not say so. Ignored for a single file
    #[arg(long = "hive", action, help_heading = "Open")]
    pub hive: bool,

    /// Compression, when the extension does not say: gzip, zstd, bzip2 or xz
    #[arg(
        long = "compression",
        value_name = "C",
        value_enum,
        hide_possible_values = true,
        help_heading = "Open"
    )]
    pub compression: Option<CompressionFormat>,

    /// A dictionary to decode with, over those on the format search path: QuickFIX XML (.xml) for FIX logs, DBC (.dbc) for CAN logs, or TOML with kind = "fix" or "dbc". Repeatable
    #[arg(long = "dict", value_name = "FILE", help_heading = "Open")]
    pub dict: Vec<std::path::PathBuf>,

    /// Follow the file as it grows, as tail -f does: a local CSV, TSV, PSV or NDJSON file or Arrow IPC stream, or standard input (-). t pauses and resumes; Esc stops
    #[arg(short = 'f', long = "follow", action, help_heading = "Open")]
    pub follow: bool,

    /// Record standard input to FILE while viewing it. With -, pass it on to standard output, as tee does, and draw on the terminal
    #[arg(long = "tee", value_name = "FILE", help_heading = "Open")]
    pub tee: Option<std::path::PathBuf>,

    /// With --tee: keep FILE exactly as the bytes came. Otherwise a stream that left its header's sizes blank has them filled in when it ends
    #[arg(long = "tee-raw", requires = "tee", action, help_heading = "Open")]
    pub tee_raw: bool,

    /// With --tee: replace FILE if it is there
    #[arg(long = "force", action, requires = "tee", help_heading = "Open")]
    pub force: bool,

    /// Open in the hex view, whatever the file holds
    #[arg(long = "hex", action, help_heading = "Open")]
    pub hex: bool,

    /// Bytes a row of the hex view holds, so records line up (default: 8, 16, 32 or 64, as many as fit)
    #[arg(long = "hex-width", value_name = "N", value_parser = clap::value_parser!(u16).range(1..=4096), help_heading = "Open")]
    pub hex_width: Option<u16>,

    /// Apply a saved view by name once the data is on screen
    #[arg(long = "view", value_name = "NAME", help_heading = "Open")]
    pub view: Option<String>,

    #[arg(long = "temp-dir", value_name = "DIR", help = settings::flag_help("temp-dir"), help_heading = "Open")]
    pub temp_dir: Option<std::path::PathBuf>,

    /// Column separator: one character, tab, \t or a code such as 0x1f (default: , for .csv, tab for .tsv, | for .psv)
    #[arg(long = "delimiter", value_name = "C", value_parser = parse_delimiter, help_heading = "Delimited text")]
    pub delimiter: Option<u8>,

    /// Read the first row as data; columns are named column_1, column_2, ...
    #[arg(long = "no-header", action, help_heading = "Delimited text")]
    pub no_header: bool,

    /// The line, or comma-separated lines, holding the header, counted from 1 before anything is skipped. Several are joined per column ([csv] header_join); the data starts after the last
    #[arg(
        long = "header-rows",
        value_name = "N[,M...]",
        value_delimiter = ',',
        value_parser = clap::value_parser!(u64).range(1..),
        help_heading = "Delimited text"
    )]
    pub header_rows: Vec<u64>,

    /// Skip this many rows at the end, such as a footer. Reads the whole file to count rows
    #[arg(
        long = "footer-rows",
        value_name = "N",
        help_heading = "Delimited text"
    )]
    pub footer_rows: Option<usize>,

    /// Skip this many rows at the start; the header is read after them. Quote-aware, unlike --skip-lines
    #[arg(long = "skip-rows", value_name = "N", help_heading = "Delimited text")]
    pub skip_rows: Option<usize>,

    /// Skip this many raw lines at the start, split on newlines alone: a newline inside quotes counts
    #[arg(long = "skip-lines", value_name = "N", help_heading = "Delimited text")]
    pub skip_lines: Option<usize>,

    #[arg(long = "comment", value_name = "PREFIX", value_parser = parse_comment_char, help = settings::flag_help("comment"), help_heading = "Delimited text")]
    pub comment: Option<String>,

    #[arg(long = "skip-initial-space", value_name = "BOOL", num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = settings::parse_bool, help = settings::flag_help("skip-initial-space"), help_heading = "Delimited text")]
    pub skip_initial_space: Option<bool>,

    #[arg(long = "null", value_name = "VAL", help = settings::flag_help("null"), help_heading = "Delimited text")]
    pub null: Vec<String>,

    #[arg(long = "infer-types", value_name = "COLS|off", num_args = 0..=1, require_equals = true, default_missing_value = "all", value_parser = parse_infer_types, help = settings::flag_help("infer-types"), help_heading = "Delimited text")]
    pub infer_types: Option<InferTypes>,

    #[arg(long = "infer-rows", value_name = "N", help = settings::flag_help("infer-rows"), help_heading = "Delimited text")]
    pub infer_rows: Option<usize>,

    #[arg(long = "ignore-errors", value_name = "BOOL", num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = settings::parse_bool, help = settings::flag_help("ignore-errors"), help_heading = "Delimited text")]
    pub ignore_errors: Option<bool>,

    #[arg(long = "row-numbers", value_name = "BOOL", num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = settings::parse_bool, help = settings::flag_help("row-numbers"), help_heading = "Display")]
    pub row_numbers: Option<bool>,

    #[arg(long = "number-format", value_name = "F", value_parser = clap::builder::PossibleValuesParser::new(NUMBER_FORMAT_VALUES), hide_possible_values = true, help = settings::flag_help("number-format"), help_heading = "Display")]
    pub number_format: Option<String>,

    #[arg(long = "mouse", value_name = "BOOL", num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = settings::parse_bool, help = settings::flag_help("mouse"), help_heading = "Display")]
    pub mouse: Option<bool>,

    #[arg(long = "sample-rows", value_name = "N", help = settings::flag_help("sample-rows"), help_heading = "Display")]
    pub sample_rows: Option<usize>,

    /// Set a config key for this run, as in the file: -c display.row_numbers=true. Repeatable; a flag of the key's own still wins. `datui config keys` lists them
    #[arg(
        short = 'c',
        long = "config",
        value_name = "KEY=VALUE",
        global = true,
        help_heading = "Config"
    )]
    pub config: Vec<settings::Override>,

    #[arg(long = "log-file", value_name = "PATH", help = settings::flag_help("log-file"), help_heading = "Logging")]
    pub log_file: Option<std::path::PathBuf>,

    #[arg(long = "log-level", value_name = "LEVEL", value_parser = clap::builder::PossibleValuesParser::new(LOG_LEVELS), hide_possible_values = true, help = settings::flag_help("log-level"), help_heading = "Logging")]
    pub log_level: Option<String>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

/// The levels `--log-level` and `DATUI_LOG` take.
pub const LOG_LEVELS: &[&str] = &["error", "warn", "info", "debug", "trace", "off"];

/// What `--infer-types` asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InferTypes {
    /// Every string column.
    All,
    Off,
    Columns(Vec<String>),
}

/// `--infer-types`: all (the bare flag), `off`, or columns separated by commas.
fn parse_infer_types(text: &str) -> Result<InferTypes, String> {
    match text.trim() {
        "" | "all" | "true" => Ok(InferTypes::All),
        "off" | "false" | "none" => Ok(InferTypes::Off),
        cols => {
            let mut columns: Vec<String> = Vec::new();
            for col in cols.split(',').map(str::trim).filter(|c| !c.is_empty()) {
                if !columns.iter().any(|c| c == col) {
                    columns.push(col.to_string());
                }
            }
            Ok(InferTypes::Columns(columns))
        }
    }
}

/// A delimiter as people write it: `;`, `tab`, `\t`, or a byte code such as `0x1f`.
pub fn parse_delimiter(text: &str) -> Result<u8, String> {
    let byte = match text {
        "tab" | "\\t" | "\t" => b'\t',
        "space" => b' ',
        _ => {
            if let Some(hex) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
                u8::from_str_radix(hex, 16)
                    .map_err(|_| format!("\"{text}\" is not a byte code such as 0x1f"))?
            } else {
                let mut chars = text.chars();
                match (chars.next(), chars.next()) {
                    (Some(c), None) if c.is_ascii() => c as u8,
                    _ => {
                        return Err(format!(
                            "\"{text}\" is not one ASCII character, tab, \\t, or a code such as 0x1f"
                        ));
                    }
                }
            }
        }
    };
    if matches!(byte, b'\n' | b'\r' | b'"') {
        return Err(format!("{byte:#04x} cannot separate columns"));
    }
    Ok(byte)
}

/// What `--format` names: a format datui reads, a format spec on the search path, or
/// a spec's file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatChoice {
    Builtin(FileFormat),
    /// A spec on the search path, by its namespaced name.
    Spec(String),
    /// A spec's file: `./acme.toml`.
    File(std::path::PathBuf),
}

impl FormatChoice {
    /// The built-in format, when that is what was named.
    pub fn builtin(&self) -> Option<FileFormat> {
        match self {
            Self::Builtin(format) => Some(*format),
            Self::Spec(_) | Self::File(_) => None,
        }
    }

    /// How a file read this way is read when opened: a built-in format's
    /// [`FileFormat::read_mode`], or a spec's, whose records are decoded from a map of
    /// the file (or of its decompressed copy) only where they are shown.
    pub fn read_mode(&self, stored: Stored) -> Option<ReadMode> {
        match self {
            Self::Builtin(format) => format.read_mode(stored),
            Self::Spec(_) | Self::File(_) => match stored {
                Stored::Plain => Some(ReadMode::Lazy),
                Stored::Compressed { .. } => Some(ReadMode::Decompressed),
                Stored::Stream => None,
            },
        }
    }

    /// How one object read this way in a bucket is read. A spec reads local files, so
    /// a spec's object is downloaded first.
    pub fn bucket_object(&self, stored: Stored) -> RemoteRead {
        match self {
            Self::Builtin(format) => format.bucket_object(stored),
            Self::Spec(_) | Self::File(_) => RemoteRead::Downloaded,
        }
    }

    /// How one HTTP(S) file read this way is read; a spec's is downloaded first.
    pub fn http_file(&self) -> RemoteRead {
        match self {
            Self::Builtin(format) => format.http_file(),
            Self::Spec(_) | Self::File(_) => RemoteRead::Downloaded,
        }
    }

    /// How a bucket prefix read this way is read as one table; a spec's is not.
    pub fn bucket_prefix(&self, stored: Stored) -> Option<RemoteRead> {
        match self {
            Self::Builtin(format) => format.bucket_prefix(stored),
            Self::Spec(_) | Self::File(_) => None,
        }
    }

    /// The spec's name, when a spec was named.
    pub fn spec(&self) -> Option<&str> {
        match self {
            Self::Spec(name) => Some(name),
            Self::Builtin(_) | Self::File(_) => None,
        }
    }

    /// The spec's file, when a file was named.
    pub fn spec_file(&self) -> Option<&std::path::Path> {
        match self {
            Self::File(path) => Some(path),
            Self::Builtin(_) | Self::Spec(_) => None,
        }
    }
}

/// A built-in format's name, a spec's (namespaced, `vendor.format`, so one is never
/// taken for the other), or a spec's file: a path only if it has a `/` or ends
/// `.toml`, so `./vendor` names a file and `vendor.format` a spec.
fn parse_format(text: &str) -> Result<FormatChoice, String> {
    let is_path = text.contains('/')
        || (cfg!(windows) && text.contains('\\'))
        || text.to_ascii_lowercase().ends_with(".toml");
    if is_path {
        return Ok(FormatChoice::File(std::path::PathBuf::from(text)));
    }
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
    let file = if std::path::Path::new(text).is_file() {
        format!(" A spec file in this directory needs ./ in front: --format ./{text}.")
    } else {
        String::new()
    };
    Err(format!(
        "\"{text}\" is not a format: {}, a spec's name (`datui formats` lists them), or a spec file (a path with a / or ending .toml).{file}",
        names.join(", ")
    ))
}

/// Commands besides opening data.
#[derive(Clone, Debug, Subcommand)]
pub enum Command {
    /// List the format specs and dictionaries (FIX, DBC) on the search path: each one's name, what it matches, its file, and the copies it overrides
    #[command(after_help = command_examples("formats"))]
    Formats {
        #[command(subcommand)]
        action: Option<FormatsAction>,
    },
    /// Write the default config file, list the files read, or list every key
    #[command(after_help = command_examples("config"))]
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Show the catalogs of named datasets on the home screen, or check a catalog file
    #[command(after_help = command_examples("catalog"))]
    Catalog {
        #[command(subcommand)]
        action: CatalogAction,
    },
    /// List the themes, built in and in the config directory's themes/, or print one as a file to start from
    #[command(after_help = command_examples("theme"))]
    Theme {
        #[command(subcommand)]
        action: ThemeAction,
    },
    /// Clear the cache: recents, history, schemas and copies
    #[command(after_help = command_examples("cache"))]
    Cache {
        #[command(subcommand)]
        action: CacheAction,
    },
    /// List or remove saved views
    #[command(after_help = command_examples("views"))]
    Views {
        #[command(subcommand)]
        action: ViewsAction,
    },
    /// Print the shell completion script for SHELL
    #[command(after_help = command_examples("completions"))]
    Completions {
        #[arg(value_name = "SHELL")]
        shell: clap_complete::Shell,
    },
    /// Show a manual page, list them, or write them all under a directory
    #[command(after_help = command_examples("man"))]
    Man {
        /// The page: datui (the default), a command (config, catalog, theme, cache, views, formats, completions, man), config.5, keys, query or formats.7
        #[arg(value_name = "PAGE")]
        page: Option<String>,
        /// List the pages and what each covers
        #[arg(long, conflicts_with_all = ["page", "dir"])]
        list: bool,
        /// Write every page under DIR, in man1, man5 and man7, where man looks for them: ~/.local/share/man
        #[arg(long, value_name = "DIR", conflicts_with = "page")]
        dir: Option<std::path::PathBuf>,
    },
}

/// What `datui config` does.
#[derive(Clone, Debug, Subcommand)]
pub enum ConfigAction {
    /// Write the default config file, every key commented out at its default
    Init {
        /// Replace a config file that is there
        #[arg(long)]
        force: bool,
    },
    /// Print the config files read, lowest precedence first
    Path,
    /// List every key: its type, default, the value in effect and what set it
    Keys,
}

/// Each shell's completion script and the file name its shell loads it by: what a
/// package or the release archive installs.
pub const COMPLETION_FILES: &[(clap_complete::Shell, &str)] = &[
    (clap_complete::Shell::Bash, "datui.bash"),
    (clap_complete::Shell::Zsh, "_datui"),
    (clap_complete::Shell::Fish, "datui.fish"),
    (clap_complete::Shell::PowerShell, "_datui.ps1"),
    (clap_complete::Shell::Elvish, "datui.elv"),
];

/// The completion script for `shell`, built from `Args`, for `datui completions`.
pub fn completions(shell: clap_complete::Shell) -> String {
    let mut out = Vec::new();
    clap_complete::generate(shell, &mut Args::command(), "datui", &mut out);
    String::from_utf8_lossy(&out).into_owned()
}

/// What `datui catalog` does.
#[derive(Clone, Debug, Subcommand)]
pub enum CatalogAction {
    /// With NAME, print that catalog's file (public is the one datui ships); without, list the catalogs: id, label, datasets and file
    Show {
        /// The catalog's id: mine (catalog.toml), public, or a listed file's name
        #[arg(value_name = "NAME")]
        name: Option<String>,
    },
    /// Check a catalog file and list its datasets; a mistake is named by its line, with the fix, and exits non-zero
    Check {
        /// The catalog file
        #[arg(value_name = "FILE")]
        file: std::path::PathBuf,
    },
}

/// What `datui theme` does.
#[derive(Clone, Debug, Subcommand)]
pub enum ThemeAction {
    /// List the themes: name, the mode it is set for, where it comes from and its description
    List,
    /// Print a theme as a file with every slot, to save into themes/ and edit
    Show {
        /// The theme: night-market, day-market, or a file's name in themes/
        #[arg(value_name = "NAME")]
        name: String,
    },
}

/// What `datui cache` does.
#[derive(Clone, Debug, Subcommand)]
pub enum CacheAction {
    /// Delete the cache directory's contents, or with --recents only the recent datasets
    Clear {
        /// Forget the recently opened datasets and keep the rest
        #[arg(long)]
        recents: bool,
    },
}

/// What `datui views` does.
#[derive(Clone, Debug, Subcommand)]
pub enum ViewsAction {
    /// List the saved views: name, what files they match, when last used
    List,
    /// Remove one saved view by name
    Rm {
        #[arg(value_name = "NAME")]
        name: String,
    },
    /// Remove every saved view
    Clear,
}

/// What `datui formats` does besides listing.
#[derive(Clone, Debug, Subcommand)]
pub enum FormatsAction {
    /// Check a format spec or a dictionary (QuickFIX XML, DBC or TOML), by name or by file; with FILE, print its first decoded rows, or what a dictionary names in the log. Exits non-zero on an error
    Check {
        /// A format spec or dictionary on the search path, by name, or its file
        #[arg(value_name = "SPEC")]
        spec: String,
        /// A file (or directory of column files) to read with it
        #[arg(value_name = "FILE")]
        file: Option<std::path::PathBuf>,
    },
}

/// Why `c` cannot mark comment lines, if it cannot: it must be something, and on one
/// line. `--comment`, its config key and the Python option share it.
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

/// One option's line in the reference: `-f, --follow`, `--delimiter <C>`.
fn option_label(arg: &clap::Arg) -> String {
    let names = |arg: &clap::Arg| -> String {
        arg.get_value_names()
            .map(|names| {
                names
                    .iter()
                    .map(|n| format!("<{}>", n.as_str()))
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_default()
    };
    if arg.is_positional() {
        return if arg.is_required_set() {
            names(arg)
        } else {
            format!("[{}]...", names(arg))
        };
    }
    let mut parts = Vec::new();
    if let Some(s) = arg.get_short() {
        parts.push(format!("-{s}"));
    }
    if let Some(l) = arg.get_long() {
        parts.push(format!("--{l}"));
    }
    let op = parts.join(", ");
    let value = if arg.get_action().takes_values() {
        names(arg)
    } else {
        String::new()
    };
    if value.is_empty() {
        op
    } else if arg.get_num_args().is_some_and(|n| n.min_values() == 0) {
        // The value is optional and, where one is given, spelled with `=`.
        format!("{op}[={value}]")
    } else {
        format!("{op} {value}")
    }
}

/// `docs/reference/command-line-options.md`: the usage, every option grouped as
/// `--help` groups it, the commands and the examples. Written by `gen_docs`; a test
/// fails while the committed page differs.
pub fn render_options_markdown() -> String {
    let mut cmd = Args::command();
    cmd.build();

    let mut out = String::from(
        "# Command-line options\n\n\
         <!-- Generated from crates/datui-cli by `gen_docs`. Do not edit. -->\n\n\
         `datui --help` prints these; `datui COMMAND --help` a command's own.\n\n```text\n",
    );
    out.push_str(&cmd.render_usage().to_string());
    out.push_str("\n```\n");

    // Groups in the order `--help` shows them: the operands and ungrouped options first.
    let mut groups: Vec<Option<String>> = vec![None];
    for arg in cmd.get_arguments() {
        let heading = arg.get_help_heading().map(str::to_string);
        if !groups.contains(&heading) {
            groups.push(heading);
        }
    }
    for group in groups {
        let args: Vec<&clap::Arg> = cmd
            .get_arguments()
            .filter(|a| a.get_help_heading().map(str::to_string) == group)
            .filter(|a| !a.is_hide_set())
            .filter(|a| !matches!(a.get_id().as_str(), "help" | "version"))
            .collect();
        if args.is_empty() {
            continue;
        }
        out.push_str(&format!(
            "\n## {}\n\n| Option | Description |\n|---|---|\n",
            group.as_deref().unwrap_or("Arguments")
        ));
        for arg in args {
            let help = arg
                .get_help()
                .map(|h| escape_table_cell(&h.to_string()))
                .unwrap_or_default();
            out.push_str(&format!(
                "| `{}` | {help} |\n",
                escape_table_cell(&option_label(arg))
            ));
        }
    }
    out.push_str("\n`-h`, `--help` prints help; `-V`, `--version` the version.\n");

    out.push_str("\n## Commands\n\n| Command | Does |\n|---|---|\n");
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
            let command = format!(
                "datui {} {} {}",
                sub.get_name(),
                action.get_name(),
                operands.join(" ")
            );
            out.push_str(&format!(
                "| `{}` | {} |\n",
                command.trim_end(),
                about(action)
            ));
        }
    }

    out.push_str("\n## Examples\n\n| Command | Does |\n|---|---|\n");
    for example in examples_of("datui.1") {
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

    /// Every example parses as a command line, as written: its flags exist and take
    /// what it gives them. The runner checks that each does what it says.
    #[test]
    fn every_example_parses_as_written() {
        let examples = examples();
        assert!(examples.len() >= 4);
        for example in &examples {
            assert!(!example.description.is_empty(), "{example:?}");
            assert!(
                matches!(
                    example.expect.as_deref(),
                    None | Some("rows" | "screen" | "exit")
                ),
                "{example:?}"
            );
            // Every datui command in it: after a pipe or `&&`, behind `VAR=value`,
            // up to a redirection.
            let mut any = false;
            for part in example.command.split("&&").flat_map(|p| p.split(" | ")) {
                let mut words = shell_words(part);
                while words
                    .first()
                    .is_some_and(|w| w.contains('=') && !w.starts_with('-'))
                {
                    words.remove(0);
                }
                if let Some(at) = words.iter().position(|w| w == ">") {
                    words.truncate(at);
                }
                if words.first().map(String::as_str) != Some("datui") {
                    continue;
                }
                any = true;
                if let Err(e) = Args::try_parse_from(&words) {
                    panic!("{}: {e}", example.command);
                }
            }
            assert!(any, "no datui command: {example:?}");
        }
        assert!(examples_help().contains("| datui"), "the help shows a pipe");
    }

    /// Split a command line as a shell does, for the quoting the examples use.
    fn shell_words(line: &str) -> Vec<String> {
        let mut words = Vec::new();
        let mut word = String::new();
        let mut quote: Option<char> = None;
        let mut any = false;
        for c in line.chars() {
            match (quote, c) {
                (Some(q), c) if c == q => quote = None,
                (Some(_), c) => word.push(c),
                (None, '\'' | '"') => {
                    quote = Some(c);
                    any = true;
                }
                (None, c) if c.is_whitespace() => {
                    if any || !word.is_empty() {
                        words.push(std::mem::take(&mut word));
                        any = false;
                    }
                }
                (None, c) => word.push(c),
            }
        }
        if any || !word.is_empty() {
            words.push(word);
        }
        words
    }

    /// `-` is a path like any other to the parser: standard input, with the reading
    /// flags beside it.
    #[test]
    fn a_dash_names_standard_input() {
        let args = Args::try_parse_from(["datui", "-", "--format", "jsonl"]).unwrap();
        assert_eq!(args.paths, vec![std::path::PathBuf::from("-")]);
        assert_eq!(args.format, Some(FormatChoice::Builtin(FileFormat::Jsonl)));
        let args = Args::try_parse_from(["datui", "--delimiter", ";", "-"]).unwrap();
        assert_eq!(args.paths, vec![std::path::PathBuf::from("-")]);
        assert_eq!(args.delimiter, Some(b';'));
    }

    /// `--format` takes a built-in format, a spec's namespaced name, or a spec's file:
    /// a path only with a `/` or a `.toml` ending.
    #[test]
    fn a_format_is_built_in_a_spec_name_or_a_spec_file() {
        let format = |value: &str| {
            Args::try_parse_from(["datui", "x", "--format", value])
                .map(|a| a.format.unwrap())
                .map_err(|e| e.to_string())
        };
        assert_eq!(
            format("acme.l2feed"),
            Ok(FormatChoice::Spec("acme.l2feed".into()))
        );
        assert_eq!(format("CSV"), Ok(FormatChoice::Builtin(FileFormat::Csv)));
        assert_eq!(format("./acme"), Ok(FormatChoice::File("./acme".into())));
        assert_eq!(
            format("acme.toml"),
            Ok(FormatChoice::File("acme.toml".into()))
        );
        assert_eq!(
            format("specs/acme.TOML"),
            Ok(FormatChoice::File("specs/acme.TOML".into()))
        );
        let refused = format("cvs").unwrap_err().to_string();
        assert!(
            refused.contains("a spec's name") && refused.contains("ending .toml"),
            "{refused}"
        );
        // No spec of that name ships, so the user-facing text names none.
        assert!(!refused.contains("acme"), "{refused}");
        let args = Args::try_parse_from(["datui", "x", "-F", "parquet"]).unwrap();
        assert_eq!(
            args.format,
            Some(FormatChoice::Builtin(FileFormat::Parquet))
        );
    }

    /// A flag whose value is optional takes it only after `=`, so the path after it
    /// stays a path.
    #[test]
    fn an_optional_value_needs_equals() {
        let args = Args::try_parse_from(["datui", "--infer-types", "data.csv"]).unwrap();
        assert_eq!(args.infer_types, Some(InferTypes::All));
        assert_eq!(args.paths, vec![std::path::PathBuf::from("data.csv")]);
        let args = Args::try_parse_from(["datui", "--infer-types=off", "d.csv"]).unwrap();
        assert_eq!(args.infer_types, Some(InferTypes::Off));
        let args = Args::try_parse_from(["datui", "--infer-types=a, b,a", "d.csv"]).unwrap();
        assert_eq!(
            args.infer_types,
            Some(InferTypes::Columns(vec!["a".into(), "b".into()]))
        );
        for flag in [
            "--row-numbers",
            "--mouse",
            "--skip-initial-space",
            "--ignore-errors",
        ] {
            let args = Args::try_parse_from(["datui", flag, "data.csv"]).unwrap();
            assert_eq!(
                args.paths,
                vec![std::path::PathBuf::from("data.csv")],
                "{flag}"
            );
            let off = Args::try_parse_from(["datui", &format!("{flag}=false"), "d.csv"]).unwrap();
            assert_eq!(off.paths.len(), 1, "{flag}");
        }
        let args = Args::try_parse_from(["datui", "--mouse=false"]).unwrap();
        assert_eq!(args.mouse, Some(false));
        let args = Args::try_parse_from(["datui", "--row-numbers"]).unwrap();
        assert_eq!(args.row_numbers, Some(true));
    }

    #[test]
    fn a_delimiter_is_written_as_people_write_it() {
        for (text, byte) in [
            (";", b';'),
            ("tab", b'\t'),
            ("\\t", b'\t'),
            ("0x1f", 0x1f),
            ("|", b'|'),
        ] {
            assert_eq!(parse_delimiter(text), Ok(byte), "{text}");
        }
        for refused in ["59", "ab", "é", "0xzz", "\""] {
            assert!(parse_delimiter(refused).is_err(), "{refused}");
        }
    }

    /// Every subcommand parses; any other first word is still a path.
    #[test]
    fn every_command_parses_and_paths_stay_paths() {
        let command = |argv: &[&str]| Args::try_parse_from(argv).unwrap().command;
        assert!(matches!(
            command(&["datui", "formats"]),
            Some(Command::Formats { action: None })
        ));
        let Some(Command::Formats {
            action: Some(FormatsAction::Check { spec, file }),
        }) = command(&["datui", "formats", "check", "a.b", "f.bin"])
        else {
            panic!("a check");
        };
        assert_eq!((spec.as_str(), file), ("a.b", Some("f.bin".into())));
        assert!(matches!(
            command(&["datui", "config", "init", "--force"]),
            Some(Command::Config {
                action: ConfigAction::Init { force: true }
            })
        ));
        assert!(matches!(
            command(&["datui", "config", "path"]),
            Some(Command::Config {
                action: ConfigAction::Path
            })
        ));
        assert!(matches!(
            command(&["datui", "config", "keys"]),
            Some(Command::Config {
                action: ConfigAction::Keys
            })
        ));
        assert!(matches!(
            command(&["datui", "theme", "list"]),
            Some(Command::Theme {
                action: ThemeAction::List
            })
        ));
        assert!(matches!(
            command(&["datui", "theme", "show", "night-market"]),
            Some(Command::Theme {
                action: ThemeAction::Show { name }
            }) if name == "night-market"
        ));
        assert!(matches!(
            command(&["datui", "cache", "clear"]),
            Some(Command::Cache {
                action: CacheAction::Clear { recents: false }
            })
        ));
        assert!(matches!(
            command(&["datui", "cache", "clear", "--recents"]),
            Some(Command::Cache {
                action: CacheAction::Clear { recents: true }
            })
        ));
        assert!(matches!(
            command(&["datui", "views", "list"]),
            Some(Command::Views {
                action: ViewsAction::List
            })
        ));
        assert!(matches!(
            command(&["datui", "views", "rm", "daily"]),
            Some(Command::Views {
                action: ViewsAction::Rm { name }
            }) if name == "daily"
        ));
        assert!(matches!(
            command(&["datui", "views", "clear"]),
            Some(Command::Views {
                action: ViewsAction::Clear
            })
        ));
        let args = Args::try_parse_from(["datui", "data.csv", "--format", "./s.toml"]).unwrap();
        assert_eq!(args.paths, vec![std::path::PathBuf::from("data.csv")]);
        assert!(args.command.is_none());
    }

    /// Every shell's script names the commands and the flags.
    #[test]
    fn completions_cover_commands_and_flags() {
        use clap::ValueEnum;
        for shell in clap_complete::Shell::value_variants() {
            let script = completions(*shell);
            assert!(!script.is_empty(), "{shell}");
            assert!(script.contains("config"), "{shell}: a subcommand");
            assert!(script.contains("infer-types"), "{shell}: a flag");
        }
        let args = Args::try_parse_from(["datui", "completions", "fish"]).unwrap();
        assert!(matches!(
            args.command,
            Some(Command::Completions {
                shell: clap_complete::Shell::Fish
            })
        ));
        assert!(Args::try_parse_from(["datui", "completions", "tcsh"]).is_err());
    }

    /// Removed flags are gone, not hidden: the config key or command replaces each.
    #[test]
    fn removed_flags_are_refused() {
        for flag in [
            "--generate-config",
            "--clear-cache",
            "--clear-recents",
            "--remove-templates",
            "--s3-endpoint-url=x",
            "--s3-region=x",
            "--workaround-pivot-date-index=true",
            "--debug",
            "--sheet=x",
            "--variant=x",
            "--spec=x",
            "--fix-dict=x",
            "--dbc=x",
            "--parse-dates",
            "--parse-strings",
            "--no-parse-strings",
            "--polars-streaming",
            "--pages-lookahead=3",
            "--row-start-index=0",
            "--column-colors",
            "--align-numeric-right",
            "--cloud-discover=all",
            "--normalize",
            "--single-spine-schema",
            "--decompress-in-memory",
            "--template=x",
            "--skip-tail-rows=1",
            "--comment-char=#",
            "--infer-schema-length=9",
            "--null-value=x",
            "--record-size=4",
        ] {
            assert!(Args::try_parse_from(["datui", flag]).is_err(), "{flag}");
        }
    }

    /// Python's keywords are the registry's: one name each, and each open option's
    /// flag is a flag of `Args`.
    #[test]
    fn python_keywords_are_unique_and_open_options_are_flags() {
        let cmd = Args::command();
        let mut names: Vec<&str> = settings::OPEN.iter().map(|o| o.kwarg).collect();
        names.extend(settings::SETTINGS.iter().filter_map(|s| s.kwarg));
        let mut sorted = names.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), names.len(), "a keyword names two options");
        assert!(!names.contains(&"config"), "config is the dict of any key");
        for open in settings::OPEN {
            assert!(
                cmd.get_arguments().any(|a| a.get_long() == Some(open.flag)),
                "--{}",
                open.flag
            );
        }
    }

    /// Each registered flag is a flag of `Args`, and its help is the key's doc; no flag
    /// of `Args` claims a key the registry does not give it.
    #[test]
    fn registered_flags_are_args_and_share_the_doc() {
        let cmd = Args::command();
        for setting in settings::SETTINGS {
            let Some(flag) = setting.flag else { continue };
            let arg = cmd
                .get_arguments()
                .find(|a| a.get_long() == Some(flag))
                .unwrap_or_else(|| {
                    panic!("--{flag} is registered for {} but not an arg", setting.key)
                });
            let help = arg.get_help().map(|h| h.to_string()).unwrap_or_default();
            assert!(
                help.contains(setting.doc) && help.contains(setting.key),
                "--{flag}: {help}"
            );
        }
        for arg in cmd.get_arguments() {
            let help = arg.get_help().map(|h| h.to_string()).unwrap_or_default();
            if help.contains("[config: ") {
                let flag = arg.get_long().unwrap_or_default();
                assert!(settings::by_flag(flag).is_some(), "--{flag}");
            }
        }
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
    use super::{FileFormat, FormatChoice, ReadMode, RemoteRead, Stored, Summary};

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
                | FileFormat::Candump
                | FileFormat::Text
                | FileFormat::Journal => FileFormat::ALL.contains(&f),
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
                "candump",
                "text",
                "journal"
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
            assert_eq!(compressed(f, false), Some(Decompressed));
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
            Some(Decompressed)
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

    /// The dataset-info page's table of tabs says what each descriptor says: the
    /// format's tab of the Info panel, and what the home screen lists inside a file of
    /// it. Every format has a row, matched by its title.
    #[test]
    fn the_docs_tab_table_agrees_with_the_descriptors() {
        let page = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/user-guide/dataset-info.md");
        let text = std::fs::read_to_string(&page).expect("the dataset-info page");
        let header = "| Format | Tab | Lists inside the file |";
        let start = text.find(header).expect("the table of tabs");
        let mut seen: Vec<FileFormat> = Vec::new();
        for line in text[start..]
            .lines()
            .skip(2)
            .take_while(|l| l.starts_with('|'))
        {
            let cells: Vec<&str> = line.trim_matches('|').split(" | ").map(str::trim).collect();
            let [formats, tab, tables] = cells[..] else {
                panic!("three cells: {line}");
            };
            for title in formats.split(", ") {
                let title = title.trim_matches('*');
                let format = FileFormat::ALL
                    .into_iter()
                    .find(|f| f.title().eq_ignore_ascii_case(title))
                    .unwrap_or_else(|| panic!("{title} is a format's title"));
                seen.push(format);
                let said = match format.descriptor().summary {
                    Summary::Tab(tab) => format!("**{tab}**"),
                    Summary::None(why) => {
                        assert!(text.contains(why), "{title}: the page says why: {why}");
                        "none".to_string()
                    }
                };
                assert_eq!(tab, said, "{title}: Tab");
                let listed = format
                    .descriptor()
                    .tables
                    .as_ref()
                    .map_or("no", |_| "tables");
                assert_eq!(tables, listed, "{title}: Lists inside the file");
            }
        }
        for f in FileFormat::ALL {
            assert!(seen.contains(&f), "{} has a row", f.title());
        }
    }
}
