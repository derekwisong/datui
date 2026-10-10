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
    /// Files, directories, globs or URLs to open; files of the same shape open as one table. - reads standard input, as does no PATH when data is piped in. With no PATH and nothing piped in, datui opens the home screen
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

    /// A dictionary to decode with, ahead of those on the format search path: QuickFIX XML (.xml) for FIX logs, DBC (.dbc) for CAN logs, or TOML with kind = "fix" or "dbc". Repeatable
    #[arg(long = "dict", value_name = "FILE", help_heading = "Open")]
    pub dict: Vec<std::path::PathBuf>,

    /// Follow the file as it grows, as tail -f does: a local CSV, TSV, PSV or NDJSON file or Arrow IPC stream, or standard input (-). t pauses and resumes; Esc stops
    #[arg(short = 'f', long = "follow", action, help_heading = "Open")]
    pub follow: bool,

    /// Record standard input to FILE while viewing it. With -, pass it through to standard output, as tee does, while drawing on the terminal
    #[arg(long = "tee", value_name = "FILE", help_heading = "Open")]
    pub tee: Option<std::path::PathBuf>,

    /// With --tee: write FILE exactly as the bytes arrived. Without it, sizes a stream left blank in its header are filled in when it ends
    #[arg(long = "tee-raw", requires = "tee", action, help_heading = "Open")]
    pub tee_raw: bool,

    /// With --tee: replace FILE if it exists
    #[arg(long = "force", action, requires = "tee", help_heading = "Open")]
    pub force: bool,

    /// Open in the hex view, whatever the file holds
    #[arg(long = "hex", action, help_heading = "Open")]
    pub hex: bool,

    /// Bytes per row in the hex view, so records line up (default: 8, 16, 32 or 64, as many as fit)
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

    /// The line holding the header, or several lines separated by commas, counted from 1 before anything is skipped. Several lines are joined per column ([csv] header_join), and the data starts after the last
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

    /// Skip this many rows before the header (with --header-rows, after it). Quote-aware, unlike --skip-lines
    #[arg(long = "skip-rows", value_name = "N", help_heading = "Delimited text")]
    pub skip_rows: Option<usize>,

    /// Skip this many raw lines at the start. Every newline ends a line, even one inside quotes
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

    /// Set a config key for this run, written as in the file: -c display.row_numbers=true. Repeatable; the key's own flag, where it has one, still wins. `datui config keys` lists the keys
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
        /// Write every page under DIR, in man1, man5 and man7, where man finds them; for your user, ~/.local/share/man
        #[arg(long, value_name = "DIR", conflicts_with = "page")]
        dir: Option<std::path::PathBuf>,
    },
}

/// What `datui config` does.
#[derive(Clone, Debug, Subcommand)]
pub enum ConfigAction {
    /// Write the default config file, every key commented out at its default
    Init {
        /// Replace an existing config file
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
    /// Print catalog NAME's file (examples is the one datui ships). Without NAME, list the catalogs: id, label, datasets and file
    Show {
        /// The catalog's id: mine (catalog.toml), examples, or a listed file's name
        #[arg(value_name = "NAME")]
        name: Option<String>,
    },
    /// Check a catalog file and list its datasets. A mistake is reported with its line and the fix, and the command exits non-zero
    Check {
        /// The catalog file
        #[arg(value_name = "FILE")]
        file: std::path::PathBuf,
    },
}

/// What `datui theme` does.
#[derive(Clone, Debug, Subcommand)]
pub enum ThemeAction {
    /// List the themes: each one's name, mode, source and description
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
    /// Delete the cache directory's contents; with --recents, only the recent datasets
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
    /// Check a format spec or a dictionary (QuickFIX XML, DBC or TOML), by name or by file. With FILE, print the first rows it decodes, or what the dictionary names in that log. Exits non-zero on an error
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
mod tests;

#[cfg(test)]
mod format_tests;
