//! The option registry: every config key in one table, with its type, default, the
//! line that documents it, and the flag that sets it for one run, if one does.
//!
//! What reads a setting is generated from or checked against this table: the `-c
//! KEY=VALUE` parser, `datui config keys`, the commented file `datui config init`
//! writes, `docs/reference/settings.md`, and the flags' help. Adding an option is
//! adding an entry here and the field it fills in `datui-lib`'s config structs; a test
//! there fails until the two agree.

/// What a setting's value is. It decides how `-c` reads the text after `=`, and what
/// the reference says the key takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Bool,
    /// A whole number, 0 or more.
    Count,
    Text,
    /// A path; `~` and `$VAR` expand.
    Path,
    /// A list of strings. `-c` takes `a,b` or a TOML array.
    List,
    /// One of these words.
    Choice(&'static [&'static str]),
    /// Bytes with a unit: `512MiB`.
    Size,
    /// A duration with a unit: `250ms`.
    Duration,
    /// A color: a name, `#rrggbb` or `indexed(N)`.
    Color,
    /// A value of more than one shape, written as TOML; the text says which.
    Toml(&'static str),
    /// An array of tables, written in a file: `[[sources]]`. Not settable with `-c`.
    Tables,
}

impl Kind {
    /// What the reference says the key takes.
    pub fn describe(&self) -> String {
        match self {
            Kind::Bool => "bool".into(),
            Kind::Count => "integer".into(),
            Kind::Text => "string".into(),
            Kind::Path => "path".into(),
            Kind::List => "list".into(),
            Kind::Choice(words) => words.join(" \\| "),
            Kind::Size => "size".into(),
            Kind::Duration => "duration".into(),
            Kind::Color => "color".into(),
            Kind::Toml(shape) => (*shape).into(),
            Kind::Tables => "tables".into(),
        }
    }
}

/// A setting's default, as TOML.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefaultValue {
    Value(&'static str),
    /// No value unless one is written; the generated config shows this example.
    Unset(&'static str),
    /// A theme color: one default per `theme.mode`.
    Color {
        dark: &'static str,
        light: &'static str,
    },
}

/// One config key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Setting {
    /// The dotted key: `section.name`. A key ending `.*` stands for any name there.
    pub key: &'static str,
    pub kind: Kind,
    pub default: DefaultValue,
    /// One or two sentences: the generated config, the reference and the flag's help.
    pub doc: &'static str,
    /// The flag that sets it for one run, without `--`.
    pub flag: Option<&'static str>,
}

const fn s(key: &'static str, kind: Kind, default: DefaultValue, doc: &'static str) -> Setting {
    Setting {
        key,
        kind,
        default,
        doc,
        flag: None,
    }
}

impl Setting {
    const fn flag(mut self, flag: &'static str) -> Self {
        self.flag = Some(flag);
        self
    }

    /// The section: everything before the last dot.
    pub fn section(&self) -> &'static str {
        self.key.rsplit_once('.').map_or("", |(s, _)| s)
    }

    /// The name inside the section.
    pub fn name(&self) -> &'static str {
        self.key.rsplit_once('.').map_or(self.key, |(_, n)| n)
    }

    /// Whether `key` is this setting: itself, or a name under a `.*` key.
    pub fn matches(&self, key: &str) -> bool {
        match self.key.strip_suffix(".*") {
            Some(prefix) => key
                .strip_prefix(prefix)
                .and_then(|rest| rest.strip_prefix('.'))
                .is_some_and(|name| !name.is_empty() && !name.contains('.')),
            None => self.key == key,
        }
    }
}

use DefaultValue::{Unset, Value};
use Kind::*;

const fn color(
    key: &'static str,
    dark: &'static str,
    light: &'static str,
    doc: &'static str,
) -> Setting {
    s(key, Color, DefaultValue::Color { dark, light }, doc)
}

/// A config section: its title in the generated config and the reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Section {
    pub name: &'static str,
    pub title: &'static str,
    /// Markdown for the reference, under the title.
    pub intro: &'static str,
}

/// The sections, in the order the generated config and the reference list them.
pub const SECTIONS: &[Section] = &[
    Section {
        name: "",
        title: "Top level",
        intro: "",
    },
    Section {
        name: "file_loading",
        title: "File loading",
        intro: "Defaults for reading files. A file's layout (delimiter, header, rows to skip) is a flag for the one file, not a setting.",
    },
    Section {
        name: "display",
        title: "Display",
        intro: "",
    },
    Section {
        name: "performance",
        title: "Performance",
        intro: "",
    },
    Section {
        name: "chart",
        title: "Charts",
        intro: "",
    },
    Section {
        name: "data",
        title: "Data",
        intro: "The home screen.",
    },
    Section {
        name: "data.search",
        title: "Data search",
        intro: "Searching below the working directory as you type on the home screen.",
    },
    Section {
        name: "cloud",
        title: "Cloud",
        intro: "See [Cloud sources](cloud-sources.md) for `[[cloud.connections]]`.",
    },
    Section {
        name: "query",
        title: "Query",
        intro: "",
    },
    Section {
        name: "templates",
        title: "Views",
        intro: "",
    },
    Section {
        name: "clipboard",
        title: "Clipboard",
        intro: "How the copy dialog (`y`) reaches the system clipboard.",
    },
    Section {
        name: "debug",
        title: "Debug",
        intro: "",
    },
    Section {
        name: "theme",
        title: "Theme",
        intro: "",
    },
    Section {
        name: "theme.colors",
        title: "Colors",
        intro: "Each slot takes a name (`red`, `bright_blue`, `default`), `#rrggbb` or `indexed(0-255)`. Unset slots take the palette for `theme.mode`.",
    },
    Section {
        name: "glyphs",
        title: "Glyphs",
        intro: "",
    },
];

/// Every config key.
pub const SETTINGS: &[Setting] = &[
    // Top level
    s("import", List, Value("[]"), "Config files merged in before this one, in order; this file's own values win. Paths may be relative to this file, or use ~ and $VAR."),
    s("version", Text, Value("\"0.2\""), "Configuration format version."),
    s("formats_path", List, Value("[]"), "Directories of format specs, searched after ~/.config/datui/formats and $DATUI_FORMATS_PATH. Adds up across imports."),
    s("sources", Tables, Unset("[]"), "Named collections of datasets on the home screen; see Dataset collections."),
    // [file_loading]
    s("file_loading.parse_dates", Bool, Unset("true"), "Read CSV and JSON strings that look like dates or ISO 8601 timestamps as Date or Datetime (default true).").flag("parse-dates"),
    s("file_loading.decompress_in_memory", Bool, Unset("false"), "Decompress a compressed CSV, TSV or PSV into memory instead of to a temp file (default false).").flag("decompress-in-memory"),
    s("file_loading.temp_dir", Path, Unset("\"/tmp\""), "Directory for decompression temp files. Unset: the system's.").flag("temp-dir"),
    s("file_loading.single_spine_schema", Bool, Unset("true"), "A partitioned Parquet dataset's schema is every column any of its files has, from their footers; false lets Polars take one file's (default true).").flag("single-spine-schema"),
    s("file_loading.null_values", List, Unset("[\"NA\", \"amount=\"]"), "CSV values read as null: VAL in every column, COL=VAL in one."),
    s("file_loading.parse_strings", Bool, Unset("true"), "Trim CSV string columns and read them as dates, times, durations or numbers where they all parse (default true)."),
    s("file_loading.parse_strings_sample_rows", Count, Unset("1000"), "Rows sampled to infer string column types (default 1000)."),
    s("file_loading.infer_schema_length", Count, Unset("1000"), "Rows read to infer a CSV's column types (default 1000).").flag("infer-schema-length"),
    s("file_loading.ignore_errors", Bool, Unset("false"), "Skip CSV rows that do not parse instead of failing (default false).").flag("ignore-errors"),
    s("file_loading.comment_char", Text, Unset("\"#\""), "CSV lines starting with this are comments, before the header and among the data.").flag("comment-char"),
    s("file_loading.header_join", Text, Unset("\" \""), "Joins a column's names when --header-rows names several lines (default \" \")."),
    s("file_loading.skip_initial_space", Bool, Unset("false"), "Ignore the spaces after a CSV delimiter, so padded numbers are numbers (default false).").flag("skip-initial-space"),
    s("file_loading.follow_interval_ms", Count, Unset("250"), "--follow: milliseconds between checks for new rows, or on Linux the least time between two reads, 10 to 60000 (default 250)."),
    s("file_loading.memory_warning_mb", Count, Unset("1024"), "Ask before reading more than this many MB of a file whole into memory (JSON, Avro, ORC, Excel and the other formats read in memory); 0 never asks (default 1024)."),
    // [display]
    s("display.unicode", Choice(&["auto", "always", "never"]), Value("\"auto\""), "Box-drawing and arrow glyphs, or plain ASCII. auto uses them when the locale is UTF-8."),
    s("display.pages_lookahead", Count, Value("3"), "Pages of rows buffered ahead of the screen.").flag("pages-lookahead"),
    s("display.pages_lookback", Count, Value("3"), "Pages of rows buffered behind the screen.").flag("pages-lookback"),
    s("display.max_buffered_rows", Count, Value("100000"), "Most rows the table buffers; 0 for no limit."),
    s("display.max_buffered_mb", Count, Value("512"), "Most MiB of rows the table buffers between reads; 0 for no limit."),
    s("display.row_numbers", Bool, Value("false"), "Show row numbers on the left (# toggles).").flag("row-numbers"),
    s("display.row_start_index", Count, Value("1"), "The first row's number.").flag("row-start-index"),
    s("display.table_cell_padding", Toml("\"comfortable\" \\| \"compact\" \\| integer"), Value("\"comfortable\""), "Space between columns: comfortable (2 cells), compact (1) or a number of cells."),
    s("display.column_colors", Bool, Value("true"), "Color cells by column type.").flag("column-colors"),
    s("display.dtype_row", Bool, Value("true"), "A second header row naming each column's type (D toggles)."),
    s("display.notes_accent", Bool, Value("true"), "Accent the i key when datui has noticed something about the data."),
    s("display.mouse", Bool, Value("true"), "Take the mouse: the wheel scrolls, a click selects. false leaves it to the terminal.").flag("mouse"),
    s("display.sidebar_width", Count, Unset("70"), "Width of every sidebar, in cells. Unset: each sidebar's own."),
    s("display.align_numeric_right", Bool, Value("true"), "Right-align numeric columns and their headers.").flag("align-numeric-right"),
    s("display.number_format", Toml("preset \\| table"), Value("\"none\""), "Digit grouping: none, thousands, european, si, swiss, indian, underscore or system, or a [display.number_format] table (, toggles).").flag("number-format"),
    // [performance]
    s("performance.analysis_sample_rows", Count, Value("100000"), "Rows an analysis samples from a larger table, spread across all of it; 0 reads every row.").flag("sample-rows"),
    s("performance.polars_streaming", Bool, Value("true"), "Use the Polars streaming engine where it applies.").flag("polars-streaming"),
    s("performance.quality_local_copy_mb", Count, Value("2048"), "Most MiB a Data Quality full scan of a remote dataset copies into the cache to read once; 0 never copies."),
    // [chart]
    s("chart.row_limit", Count, Value("10000"), "Rows a chart reads; a larger table is sampled across all of it."),
    s("chart.grid", Bool, Value("false"), "Start charts with a grid at the major ticks (g toggles)."),
    // [data]
    s("data.directories", List, Value("[]"), "Directories the home screen always lists. ~ and $VAR expand."),
    s("data.use_desktop_recents", Bool, Value("true"), "Also list directories from the desktop's recently-used files; never the file names."),
    s("data.show_unreadable_files", Bool, Value("false"), "List files datui cannot read, dimmed (Ctrl+A toggles)."),
    s("data.builtin_catalog", Bool, Value("true"), "Offer the built-in public collection of datasets."),
    s("data.hide_sources", List, Value("[]"), "Collections not shown, by name. Adds up across imports."),
    s("data.preview_max_mb", Count, Value("64"), "Largest local file, in MB, whose first rows the home screen previews; 0 turns the preview off."),
    s("data.search.enabled", Bool, Value("true"), "Search below the working directory as you type."),
    s("data.search.max_depth", Count, Value("8"), "How many directories deep the search goes."),
    s("data.search.max_results", Count, Value("1000"), "Matches listed; the rest are counted."),
    s("data.search.time_budget_ms", Count, Value("1500"), "Milliseconds the search walks before keeping what it found."),
    s("data.search.cross_filesystems", Bool, Value("false"), "Descend into other filesystems, network mounts included."),
    s("data.search.follow_gitignore", Bool, Value("false"), "Skip what .gitignore ignores."),
    s("data.search.skip", List, Value("[\"node_modules\", \"target\", \"build\", \"dist\", \"vendor\", \"site-packages\", \"__pycache__\", \"venv\", \"env\"]"), "Directory names never searched. Replaces the defaults; skip_extra adds to them."),
    s("data.search.skip_extra", List, Value("[]"), "Directory names never searched, besides skip."),
    s("data.search.extensions", List, Value("[]"), "Extensions searched for; empty means every format datui opens."),
    // [cloud]
    s("cloud.s3_endpoint_url", Text, Unset("\"http://localhost:9000\""), "Endpoint for S3-compatible storage such as MinIO.").flag("s3-endpoint-url"),
    s("cloud.s3_access_key_id", Text, Unset("\"\""), "S3 access key.").flag("s3-access-key-id"),
    s("cloud.s3_secret_access_key", Text, Unset("\"\""), "S3 secret key. Prefer AWS_SECRET_ACCESS_KEY.").flag("s3-secret-access-key"),
    s("cloud.s3_region", Text, Unset("\"us-east-1\""), "S3 region.").flag("s3-region"),
    s("cloud.connections", Tables, Unset("[]"), "Cloud stores to list on the home screen; see Cloud sources."),
    s("cloud.hide", List, Unset("[]"), "Cloud source IDs not shown on the home screen. Adds up across imports."),
    s("cloud.azure_account_keys", Bool, Unset("true"), "Read an Azure account with its access keys when a sign-in has no data role (default true)."),
    s("cloud.env_files", List, Unset("[\".env\"]"), "Files to read cloud variables from, relative to the working directory. Adds up across imports."),
    s("cloud.instance_identity", Bool, Unset("false"), "Use the identity of the cloud VM datui runs on (default false)."),
    s("cloud.discover", Toml("bool \\| \"all\" \\| \"none\" \\| list"), Unset("true"), "Logins found on this machine that become home-screen sources: all, none, or kinds from s3, gcs, azure.").flag("cloud-discover"),
    s("cloud.list_on_start", Bool, Unset("false"), "List every source's buckets when the home screen opens, not when one is entered (default false)."),
    // [query]
    s("query.history_limit", Count, Value("1000"), "Queries remembered."),
    s("query.enable_history", Bool, Value("true"), "Remember queries."),
    s("query.default_mode", Choice(&["sql", "search", "q-style"]), Value("\"sql\""), "The mode / opens on when no query is active."),
    // [templates]
    s("templates.auto_apply", Bool, Value("false"), "Apply the best-matching view when a file opens."),
    // [clipboard]
    s("clipboard.backend", Choice(&["auto", "native", "osc52"]), Value("\"auto\""), "auto: the display server where one answers, osc52 elsewhere (SSH). osc52 is an escape sequence the terminal applies."),
    s("clipboard.osc52_limit_kb", Count, Value("100"), "Longest osc52 copy to attempt, in KB of base64."),
    // [debug]
    s("debug.enabled", Bool, Value("false"), "Show the debug overlay.").flag("debug"),
    s("debug.show_performance", Bool, Value("true"), "Unused."),
    s("debug.show_query", Bool, Value("true"), "Unused."),
    s("debug.show_transformations", Bool, Value("true"), "Unused."),
    s("debug.log_file", Path, Unset("\"~/datui.log\""), "Where the log goes. Unset: datui.log in the cache directory.").flag("log-file"),
    // [theme]
    s("theme.mode", Choice(&["auto", "dark", "light"]), Unset("\"auto\""), "Which built-in palette to start from. auto reads COLORFGBG and falls back to dark."),
    color("theme.colors.keybind_hints", "#7dcfff", "#2e7de9", "Keys in the control bar and dialogs."),
    color("theme.colors.keybind_labels", "#a9b1d6", "#3760bf", "Labels beside keys in the control bar."),
    color("theme.colors.throbber", "#7dcfff", "#2e7de9", "The busy spinner."),
    color("theme.colors.primary_chart_series_color", "#7dcfff", "#2e7de9", "Histogram bars, bar charts and Q-Q points."),
    color("theme.colors.secondary_chart_series_color", "#565f89", "#848cb5", "Theoretical overlays and the Q-Q reference line."),
    color("theme.colors.success", "#9ece6a", "#587539", "Success."),
    color("theme.colors.error", "#f7768e", "#f52a65", "Errors."),
    color("theme.colors.warning", "#e0af68", "#8c6c3e", "Warnings."),
    color("theme.colors.dimmed", "#565f89", "#848cb5", "Dimmed text, nulls and axes."),
    color("theme.colors.background", "default", "default", "Main background."),
    color("theme.colors.surface", "default", "default", "Dialog background."),
    color("theme.colors.controls_bg", "#262a3f", "#d0d5e3", "Control bar and count chips."),
    color("theme.colors.text_primary", "default", "default", "Text."),
    color("theme.colors.text_secondary", "#737aa2", "#6172b0", "Secondary text."),
    color("theme.colors.text_inverse", "#1a1b26", "#e1e2e7", "Text on a key chip."),
    color("theme.colors.table_header", "#c0caf5", "#3760bf", "Header text."),
    color("theme.colors.table_header_bg", "#2b3047", "#c4c8da", "Header fill."),
    color("theme.colors.row_numbers", "#565f89", "#848cb5", "The row-number column."),
    color("theme.colors.column_separator", "#3b4261", "#a8aecb", "The rule after frozen columns and beside section titles."),
    color("theme.colors.table_selected", "#283457", "#b6bfe2", "Tint under the current row; reversed swaps text and background instead."),
    color("theme.colors.column_cursor", "#292e42", "#cbd3f2", "Tint under the column cursor's cells."),
    color("theme.colors.cell_cursor", "#3b4261", "#a0aef0", "The column cursor's header and the current cell."),
    color("theme.colors.sidebar_border", "#565f89", "#6172b0", "Sidebar and dialog borders."),
    color("theme.colors.modal_border_active", "#7dcfff", "#2e7de9", "The focused dialog's border."),
    color("theme.colors.modal_border_error", "#f7768e", "#f52a65", "An error dialog's border."),
    color("theme.colors.distribution_normal", "#9ece6a", "#587539", "Analysis: a normal distribution."),
    color("theme.colors.distribution_skewed", "#e0af68", "#8c6c3e", "Analysis: a skewed distribution."),
    color("theme.colors.distribution_other", "#c0caf5", "#3760bf", "Analysis: other distributions."),
    color("theme.colors.outlier_marker", "#f7768e", "#f52a65", "Analysis: outliers."),
    color("theme.colors.cursor_focused", "default", "default", "The text cursor; default reverses the text under it."),
    color("theme.colors.cursor_dimmed", "default", "default", "Unused."),
    color("theme.colors.cursor_text", "default", "default", "Text under the cursor block; default picks black or white by contrast."),
    color("theme.colors.alternate_row_color", "#1e2030", "#dcdfea", "Every other row; default turns the stripe off."),
    color("theme.colors.str_col", "#9ece6a", "#587539", "String columns."),
    color("theme.colors.int_col", "#7aa2f7", "#2e7de9", "Integer columns."),
    color("theme.colors.float_col", "#2ac3de", "#007197", "Float columns."),
    color("theme.colors.bool_col", "#e0af68", "#8c6c3e", "Boolean columns."),
    color("theme.colors.temporal_col", "#bb9af7", "#9854f1", "Date, time and datetime columns."),
    color("theme.colors.binary_col", "#565f89", "#848cb5", "Binary columns' placeholder."),
    color("theme.colors.chart_series_color_1", "#7dcfff", "#2e7de9", "Chart series 1."),
    color("theme.colors.chart_series_color_2", "#bb9af7", "#9854f1", "Chart series 2."),
    color("theme.colors.chart_series_color_3", "#9ece6a", "#587539", "Chart series 3."),
    color("theme.colors.chart_series_color_4", "#e0af68", "#8c6c3e", "Chart series 4."),
    color("theme.colors.chart_series_color_5", "#7aa2f7", "#007197", "Chart series 5."),
    color("theme.colors.chart_series_color_6", "#f7768e", "#f52a65", "Chart series 6."),
    color("theme.colors.chart_series_color_7", "#ff9e64", "#b15c00", "Chart series 7."),
    color("theme.colors.chart_grid", "#3d4785", "#70aabf", "The chart grid, a shade dimmer than dimmed."),
    color("theme.colors.accent", "#7dcfff", "#2e7de9", "Key chips, focused titles and the selection rail."),
    color("theme.colors.accent_bright", "#a4daff", "#1a6cd0", "The section the cursor is in."),
    color("theme.colors.gradient_start", "#7aa2f7", "#2e7de9", "The wordmark's first stop."),
    color("theme.colors.gradient_end", "#bb9af7", "#9854f1", "The wordmark's last stop."),
    color("theme.colors.find_match", "#e0af68", "#f0c35a", "Behind the cell a find landed on."),
    color("theme.colors.hex_null", "#565f89", "#848cb5", "Hex view: the byte 0x00."),
    color("theme.colors.hex_printable", "#7dcfff", "#007197", "Hex view: printable ASCII."),
    color("theme.colors.hex_whitespace", "#9ece6a", "#587539", "Hex view: whitespace bytes."),
    color("theme.colors.hex_control", "#bb9af7", "#9854f1", "Hex view: other control bytes."),
    color("theme.colors.hex_high", "#e0af68", "#8c6c3e", "Hex view: 0x80 to 0xFE."),
    color("theme.colors.hex_ff", "#f7768e", "#f52a65", "Hex view: the byte 0xFF."),
    // [glyphs]
    s("glyphs.*", Toml("string \\| list"), Unset("in_object_store = \"☁\""), "A glyph slot from glyphs.rs, replaced when the Unicode set is active. Keeps the width of the glyph it replaces."),
];

/// The setting `key` names, if any.
pub fn find(key: &str) -> Option<&'static Setting> {
    SETTINGS.iter().find(|s| s.matches(key))
}

/// The setting a flag sets, if one does.
pub fn by_flag(flag: &str) -> Option<&'static Setting> {
    SETTINGS.iter().find(|s| s.flag == Some(flag))
}

/// The settings of `section`, in table order.
pub fn in_section(section: &str) -> impl Iterator<Item = &'static Setting> + '_ {
    SETTINGS.iter().filter(move |s| s.section() == section)
}

/// `docs/reference/settings.md`: every key by section, from this table. Written by
/// `gen_docs settings`; a test fails while the committed page differs.
pub fn render_settings_markdown() -> String {
    let cell = |s: &str| s.replace('|', "\\|").replace('\n', " ");
    let mut out = String::from(
        "# Settings reference\n\n\
         <!-- Generated from crates/datui-cli/src/settings.rs by `gen_docs settings`. Do not edit. -->\n\n\
         Set these in `config.toml` (`datui config init` writes one with every key\n\
         commented out), or for one run with `-c KEY=VALUE`:\n\n\
         ```bash\n\
         datui -c display.row_numbers=true data.csv\n\
         ```\n\n\
         A flag beats `-c`, which beats the config files, which beat the defaults.\n\
         `datui config keys` lists every key with its value in effect and where it was\n\
         set. See [Configure datui](../user-guide/configuration.md) for where the file\n\
         lives, imports, the theme and troubleshooting.\n\n\
         | Type | Written as |\n\
         |---|---|\n\
         | size | A number and a unit: `512MiB`, `2GiB`, `100KiB` (`MB`, `GB` are powers of 1000). `0` needs none |\n\
         | duration | A number and a unit: `250ms`, `1.5s`, `2m` |\n\
         | list | In a file, a TOML array; with `-c`, `a,b` or the array |\n\
         | color | A name (`red`, `bright_blue`, `default`), `#rrggbb` or `indexed(0-255)` |\n",
    );
    for section in SECTIONS {
        let settings: Vec<&Setting> = in_section(section.name).collect();
        if settings.is_empty() {
            continue;
        }
        out.push_str(&format!("\n## {}\n\n", section.title));
        if !section.name.is_empty() {
            out.push_str(&format!("`[{}]`", section.name));
            if !section.intro.is_empty() {
                out.push_str(&format!(" {}", section.intro));
            }
            out.push_str("\n\n");
        } else if !section.intro.is_empty() {
            out.push_str(&format!("{}\n\n", section.intro));
        }
        let colors = settings
            .iter()
            .all(|s| matches!(s.default, DefaultValue::Color { .. }));
        if colors {
            out.push_str("| Key | Dark | Light | Description |\n|---|---|---|---|\n");
        } else {
            out.push_str("| Key | Type | Default | Flag | Description |\n|---|---|---|---|---|\n");
        }
        for setting in settings {
            let key = format!("`{}`", setting.key);
            match setting.default {
                DefaultValue::Color { dark, light } => out.push_str(&format!(
                    "| {key} | `{dark}` | `{light}` | {} |\n",
                    cell(setting.doc)
                )),
                DefaultValue::Value(v) | DefaultValue::Unset(v) => {
                    let default = match setting.default {
                        DefaultValue::Value(_) => format!("`{}`", cell(v)),
                        _ => "unset".to_string(),
                    };
                    let flag = setting.flag.map(|f| format!("`--{f}`")).unwrap_or_default();
                    out.push_str(&format!(
                        "| {key} | {} | {default} | {flag} | {} |\n",
                        setting.kind.describe(),
                        cell(setting.doc)
                    ));
                }
            }
        }
    }
    out
}

/// One `-c KEY=VALUE`: a key the registry knows and its value, read for the key's
/// kind.
#[derive(Debug, Clone, PartialEq)]
pub struct Override {
    pub key: String,
    pub value: toml::Value,
}

impl std::str::FromStr for Override {
    type Err = String;

    /// `KEY=VALUE`, split at the first `=`. An unknown key names the nearest known
    /// ones; a value the key cannot take says what it takes.
    fn from_str(text: &str) -> Result<Self, String> {
        let Some((key, value)) = text.split_once('=') else {
            return Err(format!(
                "\"{text}\" is not KEY=VALUE, as in -c display.row_numbers=true"
            ));
        };
        let key = key.trim();
        let setting = find(key).ok_or_else(|| unknown_key(key))?;
        let value = parse_value(setting, value).map_err(|e| {
            format!(
                "{key}: {e} ({key} takes {})",
                setting.kind.describe().replace("\\|", "|")
            )
        })?;
        Ok(Self {
            key: key.to_string(),
            value,
        })
    }
}

/// `text` as the value of `setting`. Text needs no quotes, as with `git -c`.
pub fn parse_value(setting: &Setting, text: &str) -> Result<toml::Value, String> {
    let trimmed = text.trim();
    Ok(match setting.kind {
        Bool => toml::Value::Boolean(parse_bool(trimmed)?),
        Count => {
            let n: u64 = trimmed
                .replace('_', "")
                .parse()
                .map_err(|_| format!("\"{trimmed}\" is not a whole number"))?;
            toml::Value::Integer(i64::try_from(n).map_err(|_| format!("{n} is too large"))?)
        }
        Text | Path | Color => toml::Value::String(text.to_string()),
        List => {
            if trimmed.starts_with('[') {
                toml_value(trimmed)
                    .filter(toml::Value::is_array)
                    .ok_or_else(|| format!("\"{trimmed}\" is not a list"))?
            } else {
                toml::Value::Array(
                    trimmed
                        .split(',')
                        .map(str::trim)
                        .filter(|item| !item.is_empty())
                        .map(|item| toml::Value::String(item.to_string()))
                        .collect(),
                )
            }
        }
        Choice(words) => {
            let word = trimmed.to_ascii_lowercase();
            if !words.contains(&word.as_str()) {
                return Err(format!("\"{trimmed}\" is not one of {}", words.join(", ")));
            }
            toml::Value::String(word)
        }
        Size => {
            crate::units::parse_size(trimmed)?;
            toml::Value::String(trimmed.to_string())
        }
        Duration => {
            crate::units::parse_duration(trimmed)?;
            toml::Value::String(trimmed.to_string())
        }
        // A bool, number, list or table as TOML; anything else is a word.
        Toml(_) => toml_value(trimmed).unwrap_or_else(|| toml::Value::String(trimmed.to_string())),
        Tables => return Err("is a list of tables; write it in a config file".into()),
    })
}

/// `true`, `false` and the words people use for them.
pub fn parse_bool(text: &str) -> Result<bool, String> {
    match text.to_ascii_lowercase().as_str() {
        "true" | "yes" | "on" | "1" => Ok(true),
        "false" | "no" | "off" | "0" => Ok(false),
        _ => Err(format!("\"{text}\" is not true or false")),
    }
}

fn toml_value(text: &str) -> Option<toml::Value> {
    format!("v = {text}")
        .parse::<toml::Table>()
        .ok()
        .and_then(|mut t| t.remove("v"))
}

/// The error for a key the registry does not know, with the nearest keys.
fn unknown_key(key: &str) -> String {
    let near = suggestions(key);
    let mut message = format!("\"{key}\" is not a config key");
    if !near.is_empty() {
        message.push_str(&format!("; did you mean {}?", near.join(" or ")));
    }
    message.push_str(" `datui config keys` lists them");
    message
}

/// Keys close to `key`: a few edits away, or the same name in another section.
pub fn suggestions(key: &str) -> Vec<&'static str> {
    let name = key.rsplit_once('.').map_or(key, |(_, n)| n);
    let mut scored: Vec<(usize, &'static str)> = SETTINGS
        .iter()
        .filter(|s| !s.key.ends_with(".*"))
        .filter_map(|s| {
            let distance = edit_distance(key, s.key);
            let close = distance <= (key.len() / 4).max(2);
            (close || s.name() == name).then_some((distance, s.key))
        })
        .collect();
    scored.sort();
    scored.into_iter().take(3).map(|(_, k)| k).collect()
}

/// Levenshtein distance, by characters.
fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut diagonal = row[0];
        row[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let above = row[j + 1];
            row[j + 1] = (diagonal + usize::from(ca != *cb))
                .min(above + 1)
                .min(row[j] + 1);
            diagonal = above;
        }
    }
    row[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_override_reads_its_value_for_the_key() {
        let o: Override = "display.row_numbers=yes".parse().unwrap();
        assert_eq!(o.value, toml::Value::Boolean(true));
        let o: Override = "file_loading.comment_char=#".parse().unwrap();
        assert_eq!(o.value, toml::Value::String("#".into()));
        let o: Override = "data.directories=~/a, /b".parse().unwrap();
        assert_eq!(o.value.as_array().map(Vec::len), Some(2));
        let o: Override = "data.directories=[\"x\"]".parse().unwrap();
        assert_eq!(o.value.as_array().map(Vec::len), Some(1));
        let o: Override = "cloud.discover=s3,gcs".parse().unwrap();
        assert_eq!(o.value, toml::Value::String("s3,gcs".into()));
        let o: Override = "cloud.discover=false".parse().unwrap();
        assert_eq!(o.value, toml::Value::Boolean(false));
        let o: Override = "glyphs.spinner=[\"a\", \"b\"]".parse().unwrap();
        assert!(o.value.is_array());
        // Only the first `=` splits.
        let o: Override = "file_loading.null_values=amount=".parse().unwrap();
        assert_eq!(o.value.as_array().unwrap()[0].as_str(), Some("amount="));
    }

    #[test]
    fn an_override_refuses_with_the_way_out() {
        let e = "display.row_number=true".parse::<Override>().unwrap_err();
        assert!(e.contains("did you mean display.row_numbers"), "{e}");
        let e = "row_numbers=true".parse::<Override>().unwrap_err();
        assert!(e.contains("display.row_numbers"), "{e}");
        let e = "display.row_start_index=one"
            .parse::<Override>()
            .unwrap_err();
        assert!(
            e.contains("not a whole number") && e.contains("integer"),
            "{e}"
        );
        let e = "display.mouse=maybe".parse::<Override>().unwrap_err();
        assert!(e.contains("not true or false"), "{e}");
        let e = "display.unicode=sometimes".parse::<Override>().unwrap_err();
        assert!(e.contains("auto, always, never"), "{e}");
        let e = "sources=x".parse::<Override>().unwrap_err();
        assert!(e.contains("config file"), "{e}");
        let e = "display.mouse".parse::<Override>().unwrap_err();
        assert!(e.contains("KEY=VALUE"), "{e}");
        let e = "nothing.like.this=1".parse::<Override>().unwrap_err();
        assert!(e.contains("config keys"), "{e}");
    }

    #[test]
    fn every_key_is_listed_once_in_a_known_section() {
        for (i, setting) in SETTINGS.iter().enumerate() {
            assert!(
                SECTIONS.iter().any(|s| s.name == setting.section()),
                "{} has no section",
                setting.key
            );
            assert!(
                SETTINGS[..i].iter().all(|other| other.key != setting.key),
                "{} is listed twice",
                setting.key
            );
            assert!(!setting.doc.is_empty(), "{} has no doc", setting.key);
        }
    }

    /// The committed reference is what the registry renders.
    #[test]
    fn the_settings_reference_is_current() {
        let page = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/reference/settings.md");
        let committed = std::fs::read_to_string(&page).expect("the settings reference");
        assert!(
            committed == render_settings_markdown(),
            "docs/reference/settings.md is stale: run .venv/bin/python scripts/docs/generate_command_line_options.py --settings -o docs/reference/settings.md"
        );
    }

    #[test]
    fn a_wildcard_key_matches_one_name_below_it() {
        let glyph = find("glyphs.spinner").expect("a glyph slot");
        assert_eq!(glyph.key, "glyphs.*");
        assert!(find("glyphs").is_none());
        assert!(find("glyphs.a.b").is_none());
        assert_eq!(find("display.mouse").map(|s| s.key), Some("display.mouse"));
    }
}
