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
    /// The keyword argument of Python's `datui.view()` and `DatuiOptions`.
    pub kwarg: Option<&'static str>,
    /// The key a delimited format spec writes it as.
    pub spec: Option<&'static str>,
}

const fn s(key: &'static str, kind: Kind, default: DefaultValue, doc: &'static str) -> Setting {
    Setting {
        key,
        kind,
        default,
        doc,
        flag: None,
        kwarg: None,
        spec: None,
    }
}

impl Setting {
    const fn flag(mut self, flag: &'static str) -> Self {
        self.flag = Some(flag);
        self
    }

    const fn kwarg(mut self, kwarg: &'static str) -> Self {
        self.kwarg = Some(kwarg);
        self
    }

    const fn spec(mut self, key: &'static str) -> Self {
        self.spec = Some(key);
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
        name: "read",
        title: "Read",
        intro: "How files are read. A file's own layout (delimiter, header, rows to skip) is a flag for that file, not a setting.",
    },
    Section {
        name: "csv",
        title: "CSV",
        intro: "CSV, TSV and PSV. A [delimited format spec](../formats/format-specs.md#delimited-text) takes these keys too.",
    },
    Section {
        name: "display",
        title: "Display",
        intro: "",
    },
    Section {
        name: "performance",
        title: "Performance",
        intro: "The rows the table buffers between reads, and the engine.",
    },
    Section {
        name: "analysis",
        title: "Analysis",
        intro: "Analysis, Data Quality and charts.",
    },
    Section {
        name: "home",
        title: "Home",
        intro: "The home screen.",
    },
    Section {
        name: "home.search",
        title: "Home search",
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
        name: "views",
        title: "Views",
        intro: "",
    },
    Section {
        name: "clipboard",
        title: "Clipboard",
        intro: "How the copy dialog (`y`) reaches the system clipboard.",
    },
    Section {
        name: "formats",
        title: "Formats",
        intro: "Where [format specs](../formats/format-specs.md) and dictionaries are found.",
    },
    Section {
        name: "log",
        title: "Log",
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
    s("catalogs", Toml("list of path \\| { path, id, label }"), Value("[]"), "Catalog files elsewhere, listed on the home screen after catalog.toml and the config directory's catalogs/*.toml, each a section; see Catalogs. Each is a path, or { path, id, label } to give it another id or label. Paths may be relative to this file. Adds up across imports."),
    // [read]
    s("read.infer_types", Toml("bool \\| list of columns"), Value("true"), "Read string columns as dates, times, durations or numbers where every value parses, after trimming: true for all, false for none, or a list of columns. CSV, and dates in JSON.").flag("infer-types").kwarg("infer_types"),
    s("read.parquet_schema", Choice(&["union", "first"]), Value("\"union\""), "A partitioned Parquet dataset's schema: union is every column any file has, from their footers; first lets Polars take one file's.").kwarg("parquet_schema"),
    s("read.decompress_in_memory", Bool, Value("false"), "Decompress a compressed CSV, TSV or PSV into memory instead of to a temp file.").kwarg("decompress_in_memory"),
    s("read.temp_dir", Path, Unset("\"/tmp\""), "Directory for decompression temp files. Unset: the system's.").flag("temp-dir").kwarg("temp_dir"),
    s("read.follow_interval", Duration, Value("\"250ms\""), "With --follow, how often the file is checked for new rows, or on Linux the least time between two reads, 10ms to 1m. Appends within one interval are one refresh."),
    s("read.exact_count_files", Count, Value("50000"), "A dataset of more files than this shows a row count estimated from a sample of its footers until c in the Info panel counts it; 0 always counts."),
    s("read.memory_warning", Size, Value("\"1GiB\""), "Ask before reading more than this of a file whole into memory (JSON, Avro, ORC, Excel and the other formats read in memory); 0 never asks."),
    s("read.audio_float", Bool, Value("false"), "Show integer audio samples as float in [-1, 1].").kwarg("audio_float"),
    // [csv]
    s("csv.comment", Text, Unset("\"#\""), "Lines starting with this are comments, before the header and among the data.").flag("comment").kwarg("comment").spec("comment"),
    s("csv.header_join", Text, Value("\" \""), "Joins a column's names when --header-rows names several lines.").kwarg("header_join").spec("header_join"),
    s("csv.skip_initial_space", Bool, Value("false"), "Ignore the spaces after a delimiter, so padded numbers are numbers and a cell of spaces is null.").flag("skip-initial-space").kwarg("skip_initial_space").spec("skip_initial_space"),
    s("csv.null_values", List, Value("[]"), "Values read as null: VAL in every column, COL=VAL in column COL only. --null is repeatable and replaces this list.").flag("null").kwarg("null_values").spec("null_values"),
    s("csv.infer_rows", Count, Value("1000"), "Rows read to infer column types.").flag("infer-rows").kwarg("infer_rows"),
    s("csv.ignore_errors", Bool, Value("false"), "Skip rows that do not parse instead of failing.").flag("ignore-errors").kwarg("ignore_errors"),
    // [display]
    s("display.unicode", Choice(&["auto", "always", "never"]), Value("\"auto\""), "Box-drawing and arrow glyphs, or plain ASCII. auto uses them when the locale is UTF-8."),
    s("display.row_numbers", Toml("\"auto\" \\| bool"), Value("\"auto\""), "Number rows on the left by their place in the source, kept through a sort or filter (# toggles). auto: for text and logs; true or false: for all of them.").flag("row-numbers").kwarg("row_numbers"),
    s("display.row_numbers_start", Count, Value("1"), "The number of the source's first row.").kwarg("row_numbers_start"),
    s("display.cell_padding", Toml("\"comfortable\" \\| \"compact\" \\| integer"), Value("\"comfortable\""), "Space between columns: comfortable (2 cells), compact (1) or a number of cells."),
    s("display.column_colors", Bool, Value("true"), "Color cells by column type.").kwarg("column_colors"),
    s("display.type_row", Bool, Value("true"), "A second header row naming each column's type (D toggles)."),
    s("display.notes_accent", Bool, Value("true"), "Accent the i key when datui has noticed something about the data."),
    s("display.mouse", Bool, Value("true"), "Take the mouse: the wheel scrolls, a click selects. false leaves it to the terminal.").flag("mouse"),
    s("display.sidebar_width", Count, Unset("70"), "Width of every sidebar, in cells. Unset: each sidebar's own."),
    s("display.right_align_numbers", Bool, Value("true"), "Right-align numeric columns and their headers.").kwarg("right_align_numbers"),
    s("display.number_format", Toml("preset \\| table"), Value("\"none\""), "Digit grouping: none, thousands, european, si, swiss, indian, underscore or system, or a [display.number_format] table (, toggles).").flag("number-format").kwarg("number_format"),
    // [performance]
    s("performance.pages_ahead", Count, Value("3"), "Pages of rows buffered ahead of the screen.").kwarg("pages_ahead"),
    s("performance.pages_behind", Count, Value("3"), "Pages of rows buffered behind the screen.").kwarg("pages_behind"),
    s("performance.max_buffered_rows", Count, Value("100000"), "Most rows the table buffers between reads; 0 for no limit.").kwarg("max_buffered_rows"),
    s("performance.max_buffered", Size, Value("\"512MiB\""), "Most memory the buffered rows may take, estimated from the schema; 0 for no limit. Rounded up to whole MiB.").kwarg("max_buffered"),
    s("performance.streaming", Bool, Value("true"), "Use the Polars streaming engine where it applies.").kwarg("streaming"),
    // [analysis]
    s("analysis.sample_rows", Count, Value("100000"), "Rows an analysis samples from a larger table, spread across all of it; 0 reads every row.").flag("sample-rows").kwarg("sample_rows"),
    s("analysis.chart_rows", Count, Value("10000"), "Rows a chart reads; a larger table is sampled across all of it."),
    s("analysis.chart_grid", Bool, Value("false"), "Start charts with a grid at the major ticks (g toggles)."),
    s("analysis.quality_local_copy", Size, Value("\"2GiB\""), "Most a Data Quality full scan of a remote dataset copies into the cache to read once; 0 never copies."),
    // [home]
    s("home.desktop_recents", Bool, Value("true"), "Also list directories from the desktop's recently-used files; never the file names."),
    s("home.show_unreadable", Bool, Value("false"), "List files datui cannot read, dimmed (Ctrl+A toggles)."),
    s("home.hide", List, Value("[]"), "Catalogs not shown, by id: mine (catalog.toml), public, or a listed file's name; one entry as catalog/id, such as public/nyc-taxis. Adds up across imports."),
    s("home.preview_max", Size, Value("\"64MiB\""), "Largest local file whose first rows the home screen previews; 0 turns the preview off."),
    s("home.search.enabled", Bool, Value("true"), "Search below the working directory as you type."),
    s("home.search.max_depth", Count, Value("8"), "How many directories deep the search goes."),
    s("home.search.max_results", Count, Value("1000"), "Matches listed; the rest are counted."),
    s("home.search.time_budget", Duration, Value("\"1500ms\""), "How long the search walks before keeping what it found."),
    s("home.search.cross_filesystems", Bool, Value("false"), "Descend into other filesystems, network mounts included."),
    s("home.search.follow_gitignore", Bool, Value("false"), "Skip what .gitignore ignores."),
    s("home.search.skip", List, Value("[\"node_modules\", \"target\", \"build\", \"dist\", \"vendor\", \"site-packages\", \"__pycache__\", \"venv\", \"env\"]"), "Directory names never searched. Replaces the defaults; skip_extra adds to them."),
    s("home.search.skip_extra", List, Value("[]"), "Directory names never searched, besides skip."),
    s("home.search.extensions", List, Value("[]"), "Extensions searched for; empty means those of the formats datui reads."),
    // [cloud]
    s("cloud.connections", Tables, Unset("[]"), "Cloud stores to list on the home screen; see Cloud sources."),
    s("cloud.hide", List, Value("[]"), "Cloud source IDs not shown on the home screen. Adds up across imports."),
    s("cloud.use_azure_account_keys", Bool, Value("true"), "Read an Azure account with its access keys when a sign-in has no data role, as the Portal does."),
    s("cloud.env_files", List, Value("[]"), "Files to read cloud variables from, relative to the working directory, such as .env. Adds up across imports."),
    s("cloud.instance_identity", Bool, Value("false"), "Use the identity of the cloud VM datui runs on (EC2, GCE, Azure)."),
    s("cloud.discover", Toml("bool \\| \"all\" \\| \"none\" \\| list"), Unset("true"), "Logins found on this machine that become home-screen sources: all (unset), none, or kinds from s3, gcs, azure."),
    s("cloud.list_on_start", Bool, Value("false"), "List every source's buckets when the home screen opens, not when one is entered."),
    // [query]
    s("query.history_limit", Count, Value("1000"), "Queries remembered."),
    s("query.history", Bool, Value("true"), "Remember queries."),
    s("query.default_mode", Choice(&["sql", "q"]), Value("\"sql\""), "The language : starts in, until Ctrl+T picks another."),
    // [views]
    s("views.auto_apply", Bool, Value("false"), "Apply the best-matching view when a file opens."),
    // [clipboard]
    s("clipboard.backend", Choice(&["auto", "native", "osc52"]), Value("\"auto\""), "auto: the display server where one answers, osc52 elsewhere (SSH). osc52 is an escape sequence the terminal applies."),
    s("clipboard.osc52_limit", Size, Value("\"100KiB\""), "Longest osc52 copy to attempt, as base64. Terminals cap what they accept."),
    // [formats]
    s("formats.path", List, Value("[]"), "Directories of format specs and dictionaries, searched after ~/.config/datui/formats and $DATUI_FORMATS_PATH. Adds up across imports."),
    // [log]
    s("log.file", Path, Unset("\"~/datui.log\""), "Where the log goes. Unset: datui.log in the cache directory.").flag("log-file"),
    s("log.level", Choice(&["error", "warn", "info", "debug", "trace", "off"]), Unset("\"warn\""), "How much the log says (default warn). DATUI_LOG beats a config file's; -c and --log-level beat DATUI_LOG.").flag("log-level"),
    // [theme]
    s("theme.mode", Choice(&["auto", "dark", "light"]), Unset("\"auto\""), "Which built-in palette to start from. auto reads COLORFGBG and falls back to dark."),
    color("theme.colors.chip_key", "#7dcfff", "#2e7de9", "Keys named in the footer, dialogs, the breadcrumb and the correlation matrix."),
    color("theme.colors.chip_label", "#a9b1d6", "#3760bf", "Labels beside keys in the footer, and the footer's status."),
    color("theme.colors.throbber", "#7dcfff", "#2e7de9", "The busy spinner."),
    color("theme.colors.success", "#9ece6a", "#587539", "Success."),
    color("theme.colors.error", "#f7768e", "#f52a65", "Errors."),
    color("theme.colors.warning", "#e0af68", "#8c6c3e", "Warnings."),
    color("theme.colors.dimmed", "#565f89", "#848cb5", "Dimmed text, nulls and axes."),
    color("theme.colors.background", "default", "default", "Main background."),
    color("theme.colors.surface", "default", "default", "Dialog background."),
    color("theme.colors.controls_bg", "#262a3f", "#d0d5e3", "Count chips and dialogs' key chips."),
    color("theme.colors.text_primary", "default", "default", "Text."),
    color("theme.colors.text_secondary", "#737aa2", "#6172b0", "Secondary text."),
    color("theme.colors.text_inverse", "#1a1b26", "#e1e2e7", "Text on a key chip."),
    color("theme.colors.table_header", "#c0caf5", "#3760bf", "Header text."),
    color("theme.colors.table_header_bg", "#2b3047", "#c4c8da", "Header fill."),
    color("theme.colors.table_row_numbers", "#565f89", "#848cb5", "The row-number column."),
    color("theme.colors.table_column_separator", "#3b4261", "#a8aecb", "The rule after frozen columns and beside section titles."),
    color("theme.colors.table_selected", "#283457", "#b6bfe2", "Tint under the current row; reversed swaps text and background instead."),
    color("theme.colors.table_column_cursor", "#292e42", "#cbd3f2", "Tint under the column cursor's cells."),
    color("theme.colors.table_cell_cursor", "#3b4261", "#a0aef0", "The column cursor's header and the current cell."),
    color("theme.colors.sidebar_border", "#565f89", "#6172b0", "Sidebar and dialog borders."),
    color("theme.colors.modal_border_active", "#7dcfff", "#2e7de9", "The focused dialog's border."),
    color("theme.colors.modal_border_error", "#f7768e", "#f52a65", "An error dialog's border."),
    color("theme.colors.distribution_normal", "#9ece6a", "#587539", "Analysis: a normal distribution."),
    color("theme.colors.distribution_skewed", "#e0af68", "#8c6c3e", "Analysis: a skewed distribution."),
    color("theme.colors.distribution_other", "#c0caf5", "#3760bf", "Analysis: other distributions."),
    color("theme.colors.outlier_marker", "#f7768e", "#f52a65", "Analysis: outliers."),
    color("theme.colors.input_cursor", "default", "default", "The text caret; default reverses the text under it."),
    color("theme.colors.input_cursor_text", "default", "default", "Text under the caret block; default picks black or white by contrast."),
    color("theme.colors.table_alternate_row", "#1e2030", "#dcdfea", "Every other row; default turns the stripe off."),
    color("theme.colors.type_str", "#9ece6a", "#587539", "String columns."),
    color("theme.colors.type_int", "#7aa2f7", "#2e7de9", "Integer columns."),
    color("theme.colors.type_float", "#2ac3de", "#007197", "Float columns."),
    color("theme.colors.type_bool", "#e0af68", "#8c6c3e", "Boolean columns."),
    color("theme.colors.type_temporal", "#bb9af7", "#9854f1", "Date, time and datetime columns."),
    color("theme.colors.type_binary", "#565f89", "#848cb5", "Binary columns' placeholder."),
    color("theme.colors.chart_1", "#7dcfff", "#2e7de9", "Chart series 1; also histogram bars, bar charts and Q-Q points."),
    color("theme.colors.chart_2", "#bb9af7", "#9854f1", "Chart series 2."),
    color("theme.colors.chart_3", "#9ece6a", "#587539", "Chart series 3."),
    color("theme.colors.chart_4", "#e0af68", "#8c6c3e", "Chart series 4."),
    color("theme.colors.chart_5", "#7aa2f7", "#007197", "Chart series 5."),
    color("theme.colors.chart_6", "#f7768e", "#f52a65", "Chart series 6."),
    color("theme.colors.chart_7", "#ff9e64", "#b15c00", "Chart series 7."),
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

/// An option of one open, with no config key: it says how to read one file (#289),
/// so it is a flag, a Python keyword and, for delimited text, a spec key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOption {
    /// The flag, without `--`.
    pub flag: &'static str,
    pub kwarg: &'static str,
    pub kind: Kind,
    /// The key a delimited format spec writes it as.
    pub spec: Option<&'static str>,
}

const fn open(flag: &'static str, kwarg: &'static str, kind: Kind) -> OpenOption {
    OpenOption {
        flag,
        kwarg,
        kind,
        spec: None,
    }
}

const fn open_spec(
    flag: &'static str,
    kwarg: &'static str,
    kind: Kind,
    spec: &'static str,
) -> OpenOption {
    OpenOption {
        flag,
        kwarg,
        kind,
        spec: Some(spec),
    }
}

/// The open's own options that Python takes as keywords, beside the config keys'.
pub const OPEN: &[OpenOption] = &[
    open("format", "format", Text),
    open("table", "table", Text),
    open("hive", "hive", Bool),
    open(
        "compression",
        "compression",
        Choice(&["gzip", "zstd", "bzip2", "xz"]),
    ),
    open("dict", "dict", List),
    open("view", "view", Text),
    open_spec("delimiter", "delimiter", Text, "delimiter"),
    open("no-header", "no_header", Bool),
    open_spec("header-rows", "header_rows", List, "header_rows"),
    open("footer-rows", "footer_rows", Count),
    open("skip-rows", "skip_rows", Count),
    open_spec("skip-lines", "skip_lines", Count, "skip_lines"),
];

/// Who an environment variable belongs to, as the reference groups them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvGroup {
    /// datui's own.
    Datui,
    /// The terminal's: color, glyphs.
    Terminal,
    /// The programs datui hands a value to.
    Programs,
    /// The cloud logins, as each provider's own tools read them.
    Cloud,
}

impl EnvGroup {
    /// The group's heading in the reference.
    pub fn title(self) -> &'static str {
        match self {
            Self::Datui => "datui",
            Self::Terminal => "Terminal",
            Self::Programs => "Programs datui starts",
            Self::Cloud => "Cloud logins",
        }
    }
}

/// One environment variable datui reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvVar {
    /// The name, or names read as one (`AWS_REGION`, `AWS_DEFAULT_REGION`).
    pub names: &'static [&'static str],
    pub group: EnvGroup,
    /// What it does, as markdown.
    pub doc: &'static str,
}

const fn env(names: &'static [&'static str], group: EnvGroup, doc: &'static str) -> EnvVar {
    EnvVar { names, group, doc }
}

/// The environment variables datui reads, for the reference and the manpage.
pub const ENVIRONMENT: &[EnvVar] = &[
    env(
        &["DATUI_CONFIG_DIR"],
        EnvGroup::Datui,
        "The config directory, in place of the platform's (`~/.config/datui` on Linux). Saved views and format specs live there too",
    ),
    env(
        &["DATUI_CACHE_DIR"],
        EnvGroup::Datui,
        "The cache directory, in place of the platform's (`~/.cache/datui` on Linux)",
    ),
    env(
        &["DATUI_FORMATS_PATH"],
        EnvGroup::Datui,
        "Directories of format specs and dictionaries, separated as `PATH` is, searched before `[formats] path`",
    ),
    env(
        &["DATUI_LOG"],
        EnvGroup::Datui,
        "The log level: `error`, `warn`, `info`, `debug`, `trace` or `off`. Beats `log.level` in a file; `-c` and `--log-level` beat it",
    ),
    env(
        &["DATUI_DEBUG"],
        EnvGroup::Datui,
        "`1` shows the debug overlay",
    ),
    env(
        &["DATUI_GCP_PROJECT"],
        EnvGroup::Datui,
        "The Google Cloud project to list when projects cannot be searched, as `GOOGLE_CLOUD_PROJECT`",
    ),
    env(
        &["DATUI_TRACE_FIRST_ROWS"],
        EnvGroup::Datui,
        "A file to write the time to, in Unix nanoseconds, once the first rows are drawn. For benchmarks",
    ),
    env(
        &["NO_COLOR"],
        EnvGroup::Terminal,
        "Set to anything: no colors, the terminal's own for everything",
    ),
    env(
        &["COLORTERM", "TERM", "FORCE_COLOR"],
        EnvGroup::Terminal,
        "How many colors the terminal draws: 24-bit, 256 or 16. Theme colors are brought down to fit",
    ),
    env(
        &["COLORFGBG"],
        EnvGroup::Terminal,
        "With `theme.mode = \"auto\"`, says whether the background is light or dark",
    ),
    env(
        &["LC_ALL", "LC_CTYPE", "LANG"],
        EnvGroup::Terminal,
        "With `display.unicode = \"auto\"`, the first one set says whether the terminal takes UTF-8; when it does not, glyphs are ASCII",
    ),
    env(
        &["WT_SESSION", "TERM_PROGRAM"],
        EnvGroup::Terminal,
        "Windows only: Windows Terminal, or VS Code's terminal (`TERM_PROGRAM=vscode`), draws Unicode glyphs whatever the code page",
    ),
    env(
        &["VISUAL", "EDITOR", "PAGER"],
        EnvGroup::Programs,
        "The inspector's `o` opens text in the first one set, else `less` (on Windows, the system's opener)",
    ),
    env(
        &["AWS_PROFILE"],
        EnvGroup::Cloud,
        "The AWS profile for `s3://`, else `default`",
    ),
    env(
        &[
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ],
        EnvGroup::Cloud,
        "AWS keys, and the token of temporary ones",
    ),
    env(
        &["AWS_REGION", "AWS_DEFAULT_REGION"],
        EnvGroup::Cloud,
        "The AWS region",
    ),
    env(
        &["AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL", "AWS_ENDPOINT"],
        EnvGroup::Cloud,
        "An S3-compatible endpoint (MinIO, R2, Ceph); the first one set",
    ),
    env(
        &["AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE"],
        EnvGroup::Cloud,
        "The AWS config and credentials files, in place of `~/.aws/config` and `~/.aws/credentials`",
    ),
    env(
        &[
            "GOOGLE_APPLICATION_CREDENTIALS",
            "GOOGLE_SERVICE_ACCOUNT",
            "GOOGLE_SERVICE_ACCOUNT_PATH",
            "GOOGLE_SERVICE_ACCOUNT_KEY",
        ],
        EnvGroup::Cloud,
        "A Google Cloud service account or credentials file for `gs://`",
    ),
    env(
        &[
            "GOOGLE_CLOUD_PROJECT",
            "GCLOUD_PROJECT",
            "CLOUDSDK_CORE_PROJECT",
            "GCP_PROJECT",
        ],
        EnvGroup::Cloud,
        "The Google Cloud project to list buckets in, after `DATUI_GCP_PROJECT`; the first one set",
    ),
    env(
        &["CLOUDSDK_CONFIG"],
        EnvGroup::Cloud,
        "The `gcloud` configuration directory, in place of `~/.config/gcloud`",
    ),
    env(
        &["AZURE_STORAGE_CONNECTION_STRING"],
        EnvGroup::Cloud,
        "An Azure storage connection string, with `AccountKey` or `SharedAccessSignature`",
    ),
    env(
        &[
            "AZURE_STORAGE_ACCOUNT_NAME",
            "AZURE_STORAGE_ACCOUNT_KEY",
            "AZURE_STORAGE_SAS_TOKEN",
        ],
        EnvGroup::Cloud,
        "An Azure storage account and its key or SAS token",
    ),
    env(
        &[
            "AZURE_TENANT_ID",
            "AZURE_CLIENT_ID",
            "AZURE_CLIENT_SECRET",
            "AZURE_FEDERATED_TOKEN_FILE",
        ],
        EnvGroup::Cloud,
        "An Azure service principal, or AKS workload identity",
    ),
    env(
        &["AZURE_CONFIG_DIR"],
        EnvGroup::Cloud,
        "The Azure CLI's directory, in place of `~/.azure`",
    ),
];

/// `docs/reference/environment.md`: every variable in [`ENVIRONMENT`], by group.
pub fn render_environment_markdown() -> String {
    let cell = |s: &str| s.replace('|', "\\|").replace('\n', " ");
    let mut out = String::from(
        "# Environment variables\n\n\
         <!-- Generated from crates/datui-cli/src/settings.rs by `gen_docs`. Do not edit. -->\n\n\
         The variables datui reads.\n",
    );
    for group in [
        EnvGroup::Datui,
        EnvGroup::Terminal,
        EnvGroup::Programs,
        EnvGroup::Cloud,
    ] {
        out.push_str(&format!("\n## {}\n\n", group.title()));
        if group == EnvGroup::Cloud {
            out.push_str(
                "Read as each provider's own tools read them; a variable set but empty counts as unset. [Connect to cloud storage](../user-guide/remote-data.md) says which login wins, and `[cloud] env_files` can read them from `.env` files.\n\n",
            );
        }
        out.push_str("| Variable | What it does |\n|---|---|\n");
        for var in ENVIRONMENT.iter().filter(|v| v.group == group) {
            let names: Vec<String> = var.names.iter().map(|n| format!("`{n}`")).collect();
            out.push_str(&format!("| {} | {} |\n", names.join(", "), cell(var.doc)));
        }
    }
    out
}

/// The setting `key` names, if any.
pub fn find(key: &str) -> Option<&'static Setting> {
    SETTINGS.iter().find(|s| s.matches(key))
}

/// A flag's help: its setting's doc and key. Panics on a flag no setting names, which
/// `--help` and the tests reach.
pub fn flag_help(flag: &str) -> String {
    let setting = by_flag(flag).unwrap_or_else(|| panic!("--{flag} sets no registered key"));
    format!("{} [config: {}]", setting.doc, setting.key)
}

/// The setting a Python keyword sets, if one does.
pub fn by_kwarg(kwarg: &str) -> Option<&'static Setting> {
    SETTINGS.iter().find(|s| s.kwarg == Some(kwarg))
}

/// The setting a flag sets, if one does.
pub fn by_flag(flag: &str) -> Option<&'static Setting> {
    SETTINGS.iter().find(|s| s.flag == Some(flag))
}

/// The settings of `section`, in table order.
pub fn in_section(section: &str) -> impl Iterator<Item = &'static Setting> + '_ {
    SETTINGS.iter().filter(move |s| s.section() == section)
}

/// How a value of each type is written, as Markdown: the settings reference and
/// datui-config(5).
pub const TYPES: &[(&str, &str)] = &[
    (
        "size",
        "A number and a unit: `512MiB`, `2GiB`, `100KiB` (`MB`, `GB` are powers of 1000). `0` needs none",
    ),
    ("duration", "A number and a unit: `250ms`, `1.5s`, `2m`"),
    (
        "list",
        "In a file, a TOML array; with `-c`, `a,b` or the array",
    ),
    (
        "color",
        "A name (`red`, `bright_blue`, `default`), `#rrggbb` or `indexed(0-255)`",
    ),
];

/// `docs/reference/settings.md`: every key by section, from this table. Written by
/// `gen_docs settings`; a test fails while the committed page differs.
pub fn render_settings_markdown() -> String {
    let cell = |s: &str| s.replace('|', "\\|").replace('\n', " ");
    let mut out = String::from(
        "# Settings\n\n\
         <!-- Generated from crates/datui-cli/src/settings.rs by `gen_docs settings`. Do not edit. -->\n\n\
         Set these in `config.toml` (`datui config init` writes one with every key\n\
         commented out), or for one run with `-c KEY=VALUE`:\n\n\
         ```bash\n\
         printf 'a,b\\n1,2\\n' | datui -c display.row_numbers=true\n\
         ```\n\n\
         A flag beats `-c`, which beats the config files, which beat the defaults.\n\
         `datui config keys` lists every key with its value in effect and where it was\n\
         set. See [Configure datui](../user-guide/configuration.md) for where the file\n\
         lives, imports, the theme and troubleshooting.\n\n\
         | Type | Written as |\n\
         |---|---|\n",
    );
    for (kind, written) in TYPES {
        out.push_str(&format!("| {kind} | {written} |\n"));
    }
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
    out.push_str(
        "\nThe environment variables datui reads are in\n\
         [Environment variables](environment.md).\n",
    );
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

/// Keys close to `key`: a few edits away, or a name in another section that one of
/// them holds (`comment_char` and `comment`, so a renamed key finds its new name).
pub fn suggestions(key: &str) -> Vec<&'static str> {
    let name = key.rsplit_once('.').map_or(key, |(_, n)| n);
    let mut scored: Vec<(usize, &'static str)> = SETTINGS
        .iter()
        .filter(|s| !s.key.ends_with(".*"))
        .filter_map(|s| {
            let distance = edit_distance(key, s.key);
            let close = distance <= (key.len() / 4).max(2);
            let other = s.name();
            let alike = other == name
                || (other.len() >= 4 && name.contains(other))
                || (name.len() >= 4 && other.contains(name));
            (close || alike).then_some((distance, s.key))
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
        let o: Override = "display.mouse=yes".parse().unwrap();
        assert_eq!(o.value, toml::Value::Boolean(true));
        let o: Override = "display.row_numbers=auto".parse().unwrap();
        assert_eq!(o.value, toml::Value::String("auto".into()));
        let o: Override = "display.row_numbers=false".parse().unwrap();
        assert_eq!(o.value, toml::Value::Boolean(false));
        let o: Override = "csv.comment=#".parse().unwrap();
        assert_eq!(o.value, toml::Value::String("#".into()));
        let o: Override = "cloud.env_files=a.env, b.env".parse().unwrap();
        assert_eq!(o.value.as_array().map(Vec::len), Some(2));
        let o: Override = "home.hide=[\"x\"]".parse().unwrap();
        assert_eq!(o.value.as_array().map(Vec::len), Some(1));
        let o: Override = "cloud.discover=s3,gcs".parse().unwrap();
        assert_eq!(o.value, toml::Value::String("s3,gcs".into()));
        let o: Override = "cloud.discover=false".parse().unwrap();
        assert_eq!(o.value, toml::Value::Boolean(false));
        let o: Override = "glyphs.spinner=[\"a\", \"b\"]".parse().unwrap();
        assert!(o.value.is_array());
        // Only the first `=` splits.
        let o: Override = "csv.null_values=amount=".parse().unwrap();
        assert_eq!(o.value.as_array().unwrap()[0].as_str(), Some("amount="));
    }

    #[test]
    fn sizes_and_durations_take_their_unit() {
        let o: Override = "performance.max_buffered=1GiB".parse().unwrap();
        assert_eq!(o.value, toml::Value::String("1GiB".into()));
        let e = "performance.max_buffered=512"
            .parse::<Override>()
            .unwrap_err();
        assert!(e.contains("needs a unit"), "{e}");
        let o: Override = "read.follow_interval=1s".parse().unwrap();
        assert_eq!(o.value, toml::Value::String("1s".into()));
        let e = "read.follow_interval=fast".parse::<Override>().unwrap_err();
        assert!(e.contains("duration"), "{e}");
    }

    #[test]
    fn an_override_refuses_with_the_way_out() {
        let e = "file_loading.comment_char=#"
            .parse::<Override>()
            .unwrap_err();
        assert!(e.contains("did you mean csv.comment"), "{e}");
        let e = "performance.polars_streaming=false"
            .parse::<Override>()
            .unwrap_err();
        assert!(e.contains("performance.streaming"), "{e}");
        let e = "display.row_number=true".parse::<Override>().unwrap_err();
        assert!(e.contains("did you mean display.row_numbers"), "{e}");
        let e = "row_numbers=true".parse::<Override>().unwrap_err();
        assert!(e.contains("display.row_numbers"), "{e}");
        let e = "display.row_numbers_start=one"
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
        let e = "cloud.connections=x".parse::<Override>().unwrap_err();
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

    #[test]
    fn a_wildcard_key_matches_one_name_below_it() {
        let glyph = find("glyphs.spinner").expect("a glyph slot");
        assert_eq!(glyph.key, "glyphs.*");
        assert!(find("glyphs").is_none());
        assert!(find("glyphs.a.b").is_none());
        assert_eq!(find("display.mouse").map(|s| s.key), Some("display.mouse"));
    }
}
