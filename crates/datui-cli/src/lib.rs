//! Shared CLI definitions for datui.
//!
//! Used by the main application and by the build script (manpage) and
//! gen_docs binary (command-line-options markdown).

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use std::path::Path;

mod formats;
pub use formats::*;
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
    after_help = EXAMPLES
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

    /// Record standard input to FILE while viewing it, byte for byte. A WAV file's sizes are filled in when the stream ends. With -, pass it on to standard output, as tee does, and draw on the terminal
    #[arg(long = "tee", value_name = "FILE", help_heading = "Open")]
    pub tee: Option<std::path::PathBuf>,

    /// With --tee: leave FILE exactly as the bytes came, a WAV header's sizes included
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
    Formats {
        #[command(subcommand)]
        action: Option<FormatsAction>,
    },
    /// Write the default config file, list the files read, or list every key
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Clear the cache: recents, history, schemas and copies
    Cache {
        #[command(subcommand)]
        action: CacheAction,
    },
    /// List or remove saved views
    Views {
        #[command(subcommand)]
        action: ViewsAction,
    },
    /// Print the shell completion script for SHELL
    Completions {
        #[arg(value_name = "SHELL")]
        shell: clap_complete::Shell,
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

/// The completion script for `shell`, built from `Args`, for `datui completions`.
pub fn completions(shell: clap_complete::Shell) -> String {
    let mut out = Vec::new();
    clap_complete::generate(shell, &mut Args::command(), "datui", &mut out);
    String::from_utf8_lossy(&out).into_owned()
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
    /// Check a format spec, a QuickFIX dictionary or a DBC file, by name or by file; with FILE, print its first decoded rows, or for a dictionary what it names in the log. Exits non-zero on an error
    Check {
        /// A format spec, QuickFIX dictionary or DBC file on the search path, by name, or its file
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
        let args = Args::try_parse_from(["datui", "--delimiter", ";", "-"]).unwrap();
        assert_eq!(args.paths, vec![std::path::PathBuf::from("-")]);
        assert_eq!(args.delimiter, Some(b';'));
        assert!(EXAMPLES.contains("| datui"), "the help shows a pipe");
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
