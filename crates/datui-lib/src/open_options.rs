//! How a dataset is opened: the options a command line, the Python binding or the home
//! screen hand the load, and what a read reports back on them.

use std::path::PathBuf;
use std::sync::Arc;

use crate::config::{InferTypes, ParquetSchema};
use crate::{AppConfig, CompressionFormat, FileFormat, cli};

/// Which CSV string columns to trim and parse (date/datetime/time/duration/int/float). Default: all. None = disabled (e.g. --infer-types=off).
#[derive(Clone, Debug)]
pub enum ParseStringsTarget {
    /// Apply to all string columns.
    All,
    /// Apply only to these columns (must exist and be string type).
    Columns(Vec<String>),
}

/// Which CSV dialect options were typed on the command line. A delimited spec's
/// options replace config values but not these (#651).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TypedDialect {
    pub delimiter: bool,
    pub comment_char: bool,
    pub skip_initial_space: bool,
    pub header_rows: bool,
    pub skip_lines: bool,
}

impl TypedDialect {
    pub fn from_args(args: &cli::Args) -> Self {
        Self {
            delimiter: args.delimiter.is_some(),
            comment_char: args.comment.is_some(),
            skip_initial_space: args.skip_initial_space.is_some(),
            header_rows: !args.header_rows.is_empty(),
            skip_lines: args.skip_lines.is_some(),
        }
    }
}

#[derive(Clone)]
pub struct OpenOptions {
    pub delimiter: Option<u8>,
    pub has_header: Option<bool>,
    pub skip_lines: Option<usize>,
    pub skip_rows: Option<usize>,
    /// Skip this many rows at the end of the file (e.g. vendor footer or trailing garbage). Applied after load for CSV.
    pub skip_tail_rows: Option<usize>,
    pub compression: Option<CompressionFormat>,
    /// When set, bypass extension-based format detection and use this format (e.g. for URLs or temp files without extension).
    pub format: Option<FileFormat>,
    pub pages_lookahead: Option<usize>,
    pub pages_lookback: Option<usize>,
    pub max_buffered_rows: Option<usize>,
    pub max_buffered_mb: Option<usize>,
    pub row_numbers: bool,
    /// Neither the flag nor the config said: `#` is on for text and logs
    /// ([`crate::config::RowNumbers::Auto`]), decided once the format is known.
    pub row_numbers_auto: bool,
    pub row_start_index: usize,
    /// When true, use hive load path for directory/glob; single file uses normal load.
    pub hive: bool,
    /// Data files in the directory being opened that this read passes over, by format and
    /// count.
    ///
    /// A directory of more than one format is read as the commonest of them — a thousand
    /// CSVs and one stray JSON is a directory of CSVs — and this is what the stray was,
    /// so the dataset can say what it left out rather than the directory being refused
    /// over it. Empty for every other open, which is all of them but one.
    pub left_out: Vec<(FileFormat, usize)>,
    /// Set when the directory being opened is a lake table and this read is of its plain
    /// files: `"Delta"`, `"Iceberg"` or `"Hudi"`.
    ///
    /// The files are not the table. A delete leaves its rows on disk, an update leaves
    /// the version it replaced, and compaction leaves both sides — so this read counts
    /// rows no query of the table would return. datui does it anyway, because the
    /// alternative was a directory the user could see and could not read at all, and
    /// every other engine at least lets you look. What makes it honest rather than wrong
    /// is that it is never silent: a note and a chip in the control bar say so, and both
    /// are load-bearing.
    pub read_as_plain_files_of: Option<&'static str>,
    /// How the directory's own files differed, when they did.
    ///
    /// Only for the formats with no footer. A Parquet dataset's footers are read
    /// anyway, and say this per column and per file in far more detail — which columns,
    /// in how many files, and where — so saying it twice would be one vague note above
    /// several exact ones.
    pub files_disagree: crate::schema_union::Disagreement,
    /// When true (default), infer Hive/partitioned Parquet schema from one file for faster "Reading schema". When false, use Polars collect_schema().
    pub single_spine_schema: bool,
    /// `--view NAME`: the view to apply to the dataset named on the command
    /// line, once it is on screen. Applied to that open only; what later opens get
    /// is `[views] auto_apply`'s business.
    pub view: Option<String>,
    /// When true, CSV and JSON string columns that look like dates or ISO 8601 timestamps become Date or Datetime.
    pub parse_dates: bool,
    /// When set, trim and parse CSV string columns: None = off, Some(true) = all columns, Some(cols) = those columns only.
    pub parse_strings: Option<ParseStringsTarget>,
    /// Sample size (rows) for inferring types when parse_strings is enabled; single file or multiple/partitioned.
    pub parse_strings_sample_rows: usize,
    /// When true, decompress a compressed CSV, TSV or PSV into memory (eager read). When false (default), decompress to a temp file and use lazy scan.
    pub decompress_in_memory: bool,
    /// Directory for decompression temp files. None = system default (e.g. TMPDIR).
    pub temp_dir: Option<std::path::PathBuf>,
    /// `--table`: which table of a file that holds several: an NMEA log's sentence
    /// types, an Excel sheet (0-based index or name), a spec's variant. `None` is the
    /// file's main table.
    pub table: Option<String>,
    /// When true, use Polars streaming engine for LazyFrame collect when the streaming feature is enabled.
    pub polars_streaming: bool,
    /// Null value specs for CSV: global strings and/or "COL=VAL" for per-column. Empty = use Polars default.
    pub null_values: Option<Vec<String>>,
    /// Number of rows to use when inferring CSV schema. None = Polars default (100). Larger values reduce risk of inferring wrong type (e.g. int then N/A).
    pub infer_schema_length: Option<usize>,
    /// When true, CSV reader ignores parse errors and continues with the next batch.
    pub ignore_errors: bool,
    /// CSV lines starting with this are comments, wherever they are (`commentChar`).
    pub comment_char: Option<String>,
    /// The 1-based lines, counted from the top of the file, that hold a CSV's header
    /// (`headerRows`). Empty = Polars' own header. A layout flag, like `skip_lines`.
    pub header_rows: Vec<usize>,
    /// What joins a column's pieces when `header_rows` names several lines.
    pub header_join: String,
    /// Ignore the spaces after a CSV delimiter (`skipInitialSpace`).
    pub skip_initial_space: bool,
    /// Which dialect options were typed on the command line, so a spec leaves them.
    pub typed_dialect: TypedDialect,
    /// The debug overlay (session info, performance, query): `DATUI_DEBUG=1`.
    pub debug: bool,
    /// The split of a Hugging Face cache directory this read chose, the others and the
    /// `map()` files it left out. Found by the read, or by a bucket listing, and carried
    /// to the dataset as `left_out` is. `None` for every other open.
    pub splits: Option<Arc<crate::hf_splits::Splits>>,
    /// Where each Arrow input's rows are once its streams are converted: the IPC files
    /// read in place and the streams' rows in the converted file the scan names. Set
    /// by the load, after a conversion or a bucket's listing, never by a request.
    pub arrow_parts: Option<Arc<Vec<crate::ipc_stream::Part>>>,
    /// `--format FILE`: read the path through this format spec, whatever else
    /// matches it. A URL is fetched when the open starts, into `spec_fetched`.
    pub spec_file: Option<PathBuf>,
    /// The spec a remote `spec_file` names, fetched by the open (`Phase::ReadingSpec`).
    pub spec_fetched: Option<Arc<crate::formats::Spec>>,
    /// `--dict FILE`: FIX dictionaries and DBC files over the search path's, each
    /// taken by the reader of its kind.
    pub dicts: Vec<PathBuf>,
    /// The format spec named by `--format NAME`, or picked with `b`.
    pub spec_name: Option<String>,
    /// What a read through a format spec found, carried from the scan to the dataset.
    pub format_read: Option<Arc<crate::formats::Read>>,
    /// What the open did to the rows its reader gave — CSV column names trimmed, text
    /// columns typed — as Python method calls for Copy as Python. Found by the scan,
    /// carried to the dataset as `left_out` is. Empty for every other open.
    pub read_python: Vec<String>,
    /// `[read] audio_float`: integer audio samples as float in [-1, 1].
    pub normalize: bool,
    /// A SQLite table opened in place, carried from the scan to the dataset.
    pub sqlite: Option<Arc<SqliteOpen>>,
    /// What the reader found besides the frame: a window read straight from the file,
    /// its row count, its Info panel tab, other tables and notes. Found by the scan and
    /// carried to the dataset as `left_out` is. `None` for a reader with nothing to add.
    pub opened: Option<Arc<crate::members::Opened>>,
    /// The delimited spec the file is read through, once chosen: its dialect is in
    /// these options, and the read's units and metadata ride with it to the dataset.
    pub delimited: Option<Arc<crate::delimited_spec::DelimitedRead>>,
    /// How the scan found the data is read: lazily, through a copy, or into memory.
    /// Found by the scan and carried to the dataset for the Info panel's `Read:` line.
    pub read_mode: Option<crate::ReadMode>,
    /// The format was guessed from the first bytes of text no format's signature
    /// claims, rather than named, so a note can say how to read it otherwise.
    pub format_guessed: bool,
    /// `--hex`: show the file's bytes in the hex view, whatever it holds.
    pub hex: bool,
    /// `--hex-width N`: the bytes a row of the hex view holds.
    pub record_size: Option<usize>,
    /// `--follow`: show rows as they are appended to the file, or arrive on standard
    /// input, until stopped.
    pub follow: bool,
    /// Where the followed file's complete records end, counted by the scan and carried
    /// to the dataset as `left_out` is, for its watcher to read on from.
    pub tail: Option<Arc<crate::follow::Tail>>,
    /// Standard input still being copied to the file a follow reads.
    pub spool: Option<Arc<crate::follow::SpoolHandle>>,
    /// Standard input shown as it arrives without `--follow`: read by the follow's
    /// watcher until it ends, the view staying where it is rather than at the end.
    pub pipe: bool,
    /// `--tee FILE`: standard input is recorded to FILE, which is what is read.
    pub tee: Option<PathBuf>,
    /// `--tee-raw`: FILE is the bytes exactly as they came, a WAV header included.
    pub tee_raw: bool,
    /// `--force`: FILE may replace a file that is there.
    pub force: bool,
    /// The dataset the home screen's preview built and read the first page of, for
    /// this open to install rather than read again. Taken once.
    pub prepared: Option<crate::home_preview::Handoff>,
    /// A file of the built-in catalog: downloaded without asking when it is small.
    pub download_unasked: Option<UnaskedDownload>,
}

/// A remote file downloaded without asking: one the built-in catalog lists, at most
/// `limit` bytes by what the server says or, when it says nothing, by the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnaskedDownload {
    pub limit: u64,
    pub listed: Option<u64>,
}

impl UnaskedDownload {
    /// The largest built-in catalog file downloaded without asking.
    pub const LIMIT: u64 = 50 * 1024 * 1024;

    /// Whether a file of `size` bytes, if the server said, is downloaded unasked.
    pub fn covers(&self, size: Option<u64>) -> bool {
        size.or(self.listed)
            .is_some_and(|bytes| bytes <= self.limit)
    }
}

impl OpenOptions {
    pub fn new() -> Self {
        Self {
            delimiter: None,
            has_header: None,
            skip_lines: None,
            skip_rows: None,
            skip_tail_rows: None,
            left_out: Vec::new(),
            read_python: Vec::new(),
            read_as_plain_files_of: None,
            files_disagree: Default::default(),
            compression: None,
            format: None,
            pages_lookahead: None,
            pages_lookback: None,
            max_buffered_rows: None,
            max_buffered_mb: None,
            row_numbers: false,
            row_numbers_auto: true,
            row_start_index: 1,
            hive: false,
            single_spine_schema: true,
            view: None,
            parse_dates: true,
            parse_strings: None,
            parse_strings_sample_rows: 1000,
            decompress_in_memory: false,
            temp_dir: None,
            table: None,
            polars_streaming: true,
            null_values: None,
            infer_schema_length: None,
            ignore_errors: false,
            comment_char: None,
            header_rows: Vec::new(),
            header_join: crate::csv_dialect::DEFAULT_HEADER_JOIN.to_string(),
            skip_initial_space: false,
            typed_dialect: TypedDialect::default(),
            debug: false,
            spec_file: None,
            spec_fetched: None,
            dicts: Vec::new(),
            spec_name: None,
            format_read: None,
            normalize: false,
            sqlite: None,
            opened: None,
            splits: None,
            arrow_parts: None,
            delimited: None,
            read_mode: None,
            format_guessed: false,
            hex: false,
            record_size: None,
            follow: false,
            tail: None,
            spool: None,
            pipe: false,
            tee: None,
            tee_raw: false,
            force: false,
            prepared: None,
            download_unasked: None,
        }
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions {
    pub fn with_skip_lines(mut self, skip_lines: usize) -> Self {
        self.skip_lines = Some(skip_lines);
        self
    }

    pub fn with_skip_rows(mut self, skip_rows: usize) -> Self {
        self.skip_rows = Some(skip_rows);
        self
    }

    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.has_header = Some(has_header);
        self
    }

    /// The separator a delimited file is read with: `--delimiter` when given, else
    /// the one its format implies (`FileFormat::separator`).
    pub fn separator_or(&self, format_default: u8) -> u8 {
        self.delimiter.unwrap_or(format_default)
    }

    pub fn with_compression(mut self, compression: CompressionFormat) -> Self {
        self.compression = Some(compression);
        self
    }

    /// The lines `--header-rows` named, unless the file is being read without a
    /// header (`--no-header`, or `H`), which reads them as data.
    pub fn header_rows(&self) -> Option<&[usize]> {
        (!self.header_rows.is_empty() && self.has_header != Some(false))
            .then_some(self.header_rows.as_slice())
    }

    /// When loading CSV: use Polars try_parse_dates only if parse_strings is not set.
    /// When parse_strings is set we do our own date parsing (with strict: false), so we disable
    /// Polars' try_parse_dates to avoid "could not find an appropriate format" errors.
    pub fn csv_try_parse_dates(&self) -> bool {
        self.parse_strings.is_none() && self.parse_dates
    }

    /// The S3 settings every cloud path uses: the environment over the `[cloud]`
    /// config. `run()` folds this into the config the `App` keeps,
    /// so opening, sizing, downloading, discovery and listing all see one answer and a
    /// bucket that is listed is reached the way it will be opened. The environment is
    /// read here, not when the options are built, so a caller that starts from
    /// `OpenOptions::default()` — the Python bindings do — still honours it.
    pub fn effective_cloud(
        &self,
        cloud: &crate::config::CloudConfig,
    ) -> crate::config::CloudConfig {
        let mut merged = cloud.clone();
        merged.overlay(crate::config::CloudConfig::from_env(&crate::cloud_env::var));
        merged
    }
}

impl OpenOptions {
    /// The options the command line and the config give an open. The config has had
    /// `-c` laid over it; a flag here beats both.
    pub fn from_args_and_config(args: &cli::Args, config: &AppConfig) -> Self {
        let mut opts = OpenOptions::new();

        // A file's layout: command line only. Set in config, these applied to every
        // file opened and silently cut rows from the ones they did not describe (#289).
        opts.delimiter = args.delimiter;
        opts.skip_lines = args.skip_lines;
        opts.skip_rows = args.skip_rows;
        opts.skip_tail_rows = args.footer_rows;
        opts.has_header = args.no_header.then_some(false);
        opts.header_rows = args.header_rows.iter().map(|&n| n as usize).collect();
        opts.view = args.view.clone();
        opts.compression = args.compression;

        // A spec's name is looked up on the search path when the file is opened.
        opts.format = args.format.as_ref().and_then(cli::FormatChoice::builtin);
        opts.spec_name = args
            .format
            .as_ref()
            .and_then(|f| f.spec().map(str::to_string));
        opts.spec_file = args
            .format
            .as_ref()
            .and_then(|f| f.spec_file().map(std::path::Path::to_path_buf));
        opts.dicts = args.dict.clone();
        opts.table = args.table.clone();
        opts.hex = args.hex;
        opts.record_size = args.hex_width.map(usize::from);
        opts.hive = args.hive;
        opts.follow = args.follow;
        opts.tee = args.tee.clone();
        opts.tee_raw = args.tee_raw;
        opts.force = args.force;

        opts.pages_lookahead = Some(config.performance.pages_ahead);
        opts.pages_lookback = Some(config.performance.pages_behind);
        opts.max_buffered_rows = Some(config.performance.max_buffered_rows);
        opts.max_buffered_mb = Some(config.performance.max_buffered_mb());
        let row_numbers = args
            .row_numbers
            .map(crate::config::RowNumbers::from)
            .unwrap_or(config.display.row_numbers);
        opts.row_numbers = row_numbers == crate::config::RowNumbers::On;
        opts.row_numbers_auto = row_numbers == crate::config::RowNumbers::Auto;
        opts.row_start_index = config.display.row_numbers_start;
        opts.single_spine_schema = config.read.parquet_schema == ParquetSchema::Union;
        opts.decompress_in_memory = config.read.decompress_in_memory;
        opts.normalize = config.read.audio_float;
        opts.polars_streaming = config.performance.streaming;

        // Typing string columns, dates among them: the flag, else `read.infer_types`.
        let infer = match &args.infer_types {
            Some(cli::InferTypes::All) => InferTypes::Switch(true),
            Some(cli::InferTypes::Off) => InferTypes::Switch(false),
            Some(cli::InferTypes::Columns(cols)) => InferTypes::Columns(cols.clone()),
            None => config.read.infer_types.clone(),
        };
        (opts.parse_strings, opts.parse_dates) = match infer {
            InferTypes::Switch(false) => (None, false),
            InferTypes::Switch(true) => (Some(ParseStringsTarget::All), true),
            InferTypes::Columns(cols) => (Some(ParseStringsTarget::Columns(cols)), true),
        };

        // CSV dialect: a flag beats the config.
        let csv = &config.csv;
        opts.comment_char = args.comment.clone().or_else(|| csv.comment.clone());
        opts.header_join = csv.header_join.clone();
        opts.skip_initial_space = args.skip_initial_space.unwrap_or(csv.skip_initial_space);
        opts.typed_dialect = TypedDialect::from_args(args);
        opts.ignore_errors = args.ignore_errors.unwrap_or(csv.ignore_errors);
        // `--null` replaces the config's list, as every flag replaces its key.
        let nulls = if args.null.is_empty() {
            csv.null_values.clone()
        } else {
            args.null.clone()
        };
        opts.null_values = (!nulls.is_empty()).then_some(nulls);
        // One row count for one guess, Polars' and datui's alike.
        let infer_rows = args.infer_rows.unwrap_or(csv.infer_rows);
        opts.infer_schema_length = Some(infer_rows);
        opts.parse_strings_sample_rows = infer_rows;

        opts.temp_dir = args.temp_dir.clone().or_else(|| {
            config
                .read
                .temp_dir
                .as_deref()
                .map(crate::config::expand_config_path)
        });

        opts
    }
}

impl From<&cli::Args> for OpenOptions {
    fn from(args: &cli::Args) -> Self {
        // Use default config if creating from args alone
        let config = AppConfig::default();
        Self::from_args_and_config(args, &config)
    }
}

/// What a read of a directory found out about itself on the way through.
///
/// Filled by the pass that actually picks the files and the reader, and carried back on
/// the options so the dataset can say it in the Notes. Everything here is about what
/// datui *did*, not about what the data is — the footer notes are the other half, and
/// they are written later, by whatever read the footers.
#[derive(Debug, Clone, Default)]
pub struct ReadReport {
    /// Data files in the directory this read passed over, by format and count. A
    /// directory of more than one format is read as the commonest of them; this is the
    /// rest.
    pub left_out: Vec<(FileFormat, usize)>,
    /// How the files read differed. See [`OpenOptions::files_disagree`].
    pub files_disagree: crate::schema_union::Disagreement,
    /// The reader the files were read with, where the read chose it: a directory's
    /// commonest format, or a file's extension. Carried back as `OpenOptions::format`,
    /// so what is on screen knows whether it has a header row to turn off.
    pub format: Option<FileFormat>,
    /// What a read through a format spec found. See `OpenOptions::format_read`.
    pub format_read: Option<Arc<crate::formats::Read>>,
    /// See [`OpenOptions::read_python`].
    pub read_python: Vec<String>,
    /// A SQLite table opened in place. See `OpenOptions::sqlite`.
    pub sqlite: Option<Arc<SqliteOpen>>,
    /// See [`OpenOptions::opened`].
    pub opened: Option<Arc<crate::members::Opened>>,
    /// The split a Hugging Face cache directory was read as. See `OpenOptions::splits`.
    pub splits: Option<Arc<crate::hf_splits::Splits>>,
    /// What a read through a delimited spec found. See `OpenOptions::delimited`.
    pub delimited: Option<Arc<crate::delimited_spec::DelimitedRead>>,
    /// The table the read opened where the open named none: a database's only table.
    /// Carried back as `OpenOptions::table`, so the dataset says which table it is.
    pub table: Option<String>,
    /// The format was guessed from the text's first bytes. See
    /// [`OpenOptions::format_guessed`].
    pub guessed: bool,
}

/// A SQLite table opened in place, carried from the scan to the dataset.
pub struct SqliteOpen {
    pub pushdown: Arc<dyn crate::pushdown::Pushdown>,
    /// Taken by the dataset, which stops the table's statements when it goes.
    pub hold: std::sync::Mutex<Option<crate::sqlite::Hold>>,
    pub other_tables: Vec<String>,
}

impl std::fmt::Debug for SqliteOpen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteOpen")
            .field("other_tables", &self.other_tables)
            .finish_non_exhaustive()
    }
}
