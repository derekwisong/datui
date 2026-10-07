//! The schema of a dataset made of many files, which drift over time: every column
//! any file has, from the footers the row count already reads, with one type per
//! column:
//!
//! - equal types, or types that widen losslessly (`Int32` into `Int64`, `Float32` into
//!   `Float64`, `ms` into `ns`), become the wider one;
//! - types that do not, become the one **most rows** have. The column is not read from
//!   the other files, which is why [`DatasetSchema::omitted`] names them per file.
//!
//! Column order is the newest file's columns in its own order, then columns only older
//! files have, in the order they first appear.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use polars::chunked_array::cast::CastOptions;
use polars::prelude::{
    DataType, Field, LazyFrame, PlRefPath, PlSmallStr, PolarsResult, Schema, TimeUnit, UnionArgs,
    concat,
};

/// A footer pass in progress. Says it has finished when dropped, panic or no panic.
pub struct Pass<'a>(&'a FooterProgress);

impl Pass<'_> {
    /// One more footer read, or failed to read: both are footers no longer waited on.
    pub fn advance(&self) {
        self.0.advance();
    }
}

impl Drop for Pass<'_> {
    fn drop(&mut self) {
        self.0.done();
    }
}

/// A listing counting found objects against a [`FooterProgress`], which stops
/// reporting however it ends.
pub struct Listing<'a>(&'a FooterProgress);

impl Listing<'_> {
    /// One more object listed.
    pub fn advance(&self) {
        self.0.listed.fetch_add(1, Ordering::Relaxed);
    }

    /// The count itself, for listing tasks that outlive the borrow.
    pub fn counter(&self) -> std::sync::Arc<AtomicUsize> {
        self.0.listed.clone()
    }

    /// `n` more objects listed at once: a directory's worth.
    pub fn add(&self, n: usize) {
        self.0.listed.fetch_add(n, Ordering::Relaxed);
    }

    /// The load was abandoned: the listing stops.
    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    /// The flag itself, for listing tasks that outlive the borrow.
    pub fn cancel_flag(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        self.0.cancel_flag()
    }
}

impl Drop for Listing<'_> {
    fn drop(&mut self) {
        self.0.listing.store(false, Ordering::Release);
    }
}

/// What became of a footer pass after it finished. Public only so tests can check the
/// wiring between an open and its counter.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassCount {
    /// Passes begun against the counter.
    pub begun: usize,
    /// Footers the last pass has read.
    pub read: usize,
    /// Footers the last pass was over.
    pub total: usize,
}

/// How far a dataset's footer pass has got, for the loading screen: a climbing count
/// on a directory of thousands of files. Atomic: written only by reading threads,
/// read by the render.
#[derive(Debug, Default)]
pub struct FooterProgress {
    read: AtomicUsize,
    total: AtomicUsize,
    /// Passes begun: read and total reset when a pass ends, so this tells a finished pass
    /// from one that never started.
    passes: AtomicUsize,
    /// What the last pass was over, kept after `done` for the same reason.
    last_total: AtomicUsize,
    /// The load was abandoned: passes stop issuing reads. Shared via
    /// [`Self::cancel_flag`].
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Objects listed so far while `listing` is set: a large prefix's listing is the
    /// longest wait before any footer, with no total.
    listed: std::sync::Arc<AtomicUsize>,
    listing: std::sync::atomic::AtomicBool,
    /// Footers read at once by a pass against this counter; `0` is [`FOOTERS_AT_ONCE`].
    at_once: AtomicUsize,
    /// The row count a sample of the footers says, once one is in.
    estimate: std::sync::Mutex<Option<RowEstimate>>,
}

/// A row count estimated from a sample of footers (mean rows per file read times the
/// file count), shown as `~4.12B rows (est.)` until counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowEstimate {
    pub rows: u64,
    /// Footers the mean is over.
    pub sampled: usize,
    /// Files in the dataset.
    pub files: usize,
}

impl RowEstimate {
    /// The estimate from the `footers` read of `files` files; `None` when none were read.
    pub fn of<'a>(
        files: usize,
        footers: impl IntoIterator<Item = &'a Option<FileFooter>>,
    ) -> Option<Self> {
        let (sampled, rows) = footers
            .into_iter()
            .flatten()
            .fold((0usize, 0u128), |(n, rows), f| {
                (n + 1, rows + f.rows() as u128)
            });
        (sampled > 0).then(|| RowEstimate {
            rows: u64::try_from(rows * files as u128 / sampled as u128).unwrap_or(u64::MAX),
            sampled,
            files,
        })
    }
}

/// Footers sampled for the first estimate: a mean within a few percent for alike
/// files, in one wave or a few.
pub const ESTIMATE_SAMPLE: usize = 2_000;

/// Footers a count reads at once: each is a round trip on a store.
pub const COUNT_AT_ONCE: usize = 256;

/// `n` random indices below `files` from `seed`, ascending; all when `files <= n`.
/// The same seed draws the same sample.
pub fn random_sample(files: usize, n: usize, seed: u64) -> Vec<usize> {
    if files <= n {
        return (0..files).collect();
    }
    // splitmix64: a draw that is the same on every platform, with no dependency.
    let mut state = seed;
    let mut next = move || {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    };
    let mut chosen = std::collections::BTreeSet::new();
    while chosen.len() < n {
        chosen.insert((next() % files as u64) as usize);
    }
    chosen.into_iter().collect()
}

impl FooterProgress {
    /// Begin a listing. Its count starts from nothing and is shown until the guard drops.
    pub fn listing(&self) -> Listing<'_> {
        self.listed.store(0, Ordering::Relaxed);
        self.listing.store(true, Ordering::Release);
        Listing(self)
    }

    /// Objects listed so far while a listing is running, `None` when none is.
    pub fn listed(&self) -> Option<usize> {
        self.listing
            .load(Ordering::Acquire)
            .then(|| self.listed.load(Ordering::Relaxed))
    }

    /// Begin a pass over `total` footers. Any earlier pass's count is forgotten.
    pub fn begin(&self, total: usize) {
        self.read.store(0, Ordering::Relaxed);
        // Released after the reset and acquired in `reading`, so a render never pairs this
        // pass's total with the last one's count.
        self.last_total.store(total, Ordering::Relaxed);
        self.total.store(total, Ordering::Release);
        self.passes.fetch_add(1, Ordering::Relaxed);
    }

    /// One more footer read, or failed to read: both are footers no longer waited on.
    pub fn advance(&self) {
        self.read.fetch_add(1, Ordering::Relaxed);
    }

    /// Nothing is being waited on any more.
    pub fn done(&self) {
        self.total.store(0, Ordering::Relaxed);
    }

    /// A pass over `total` footers that reports finished however it ends, so a panic
    /// does not leave a count on screen (the render outlives the pass).
    pub fn pass(&self, total: usize) -> Pass<'_> {
        self.begin(total);
        Pass(self)
    }

    /// What became of passes on this counter: how many began, and how many footers the
    /// last read of how many. Kept after the pass so tests can check the wiring; nothing
    /// on screen reads it.
    #[doc(hidden)]
    pub fn last_pass(&self) -> PassCount {
        PassCount {
            begun: self.passes.load(Ordering::Relaxed),
            read: self.read.load(Ordering::Relaxed),
            total: self.last_total.load(Ordering::Relaxed),
        }
    }

    /// `(read, total)` while a pass is running, `None` when none is.
    pub fn reading(&self) -> Option<(usize, usize)> {
        let total = self.total.load(Ordering::Acquire);
        (total > 0).then(|| (self.read.load(Ordering::Relaxed).min(total), total))
    }

    /// The load was abandoned: passes stop issuing reads. One-way; a new load gets a new
    /// counter.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// The flag itself, for read tasks that outlive the borrow.
    pub fn cancel_flag(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        self.cancelled.clone()
    }

    /// A counter for an exact count: its passes read [`COUNT_AT_ONCE`] footers at once.
    pub fn counting() -> Self {
        let progress = Self::default();
        progress.at_once.store(COUNT_AT_ONCE, Ordering::Relaxed);
        progress
    }

    /// Footers a pass against this counter reads at once.
    pub fn reads_at_once(&self) -> usize {
        match self.at_once.load(Ordering::Relaxed) {
            0 => FOOTERS_AT_ONCE,
            n => n,
        }
    }

    /// What a sample of the footers says the row count is.
    pub fn set_estimate(&self, estimate: Option<RowEstimate>) {
        *self.estimate.lock().unwrap_or_else(|e| e.into_inner()) = estimate;
    }

    pub fn estimate(&self) -> Option<RowEstimate> {
        *self.estimate.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// What one file's footer said, short of the data: one type for every location and
/// pass.
#[derive(Debug, Clone)]
pub struct FileFooter {
    pub schema: Arc<Schema>,
    /// Rows in each row group, in file order.
    pub row_group_rows: Vec<usize>,
    /// Compressed bytes of each row group in file order: what crosses the wire, since a
    /// reader fetches whole groups, so it is what scrolling a remote dataset costs.
    pub row_group_bytes: Vec<usize>,
    /// The file's size on disk or in the store.
    pub file_bytes: usize,
    /// Uncompressed bytes of each column ([`parquet_column_bytes`]); empty where not kept
    /// (store datasets, to keep the remembered shape small).
    pub column_bytes: Vec<(String, usize)>,
}

impl FileFooter {
    /// A footer's metadata for a file of `file_bytes`, with column widths when `widths`.
    pub fn from_metadata(
        schema: Schema,
        metadata: &polars_parquet::parquet::metadata::FileMetadata,
        file_bytes: usize,
        widths: bool,
    ) -> Self {
        let column_bytes = if widths {
            parquet_column_bytes(&schema, metadata)
        } else {
            Vec::new()
        };
        FileFooter {
            schema: Arc::new(schema),
            row_group_rows: metadata.row_groups.iter().map(|rg| rg.num_rows()).collect(),
            row_group_bytes: metadata
                .row_groups
                .iter()
                .map(|rg| rg.compressed_size())
                .collect(),
            file_bytes,
            column_bytes,
        }
    }

    /// The footer at the end of `tail`, which holds the file's last bytes through its
    /// footer.
    pub fn from_tail(tail: &[u8], file_bytes: usize, widths: bool) -> color_eyre::Result<Self> {
        use polars::prelude::{ParquetReader, SchemaExt, SerReader};
        let mut cursor = std::io::Cursor::new(tail);
        let mut reader = ParquetReader::new(&mut cursor);
        let arrow_schema = reader
            .schema()
            .map_err(|e| color_eyre::eyre::eyre!("Parquet schema read failed: {e}"))?;
        let metadata = reader
            .get_metadata()
            .map_err(|e| color_eyre::eyre::eyre!("Parquet footer read failed: {e}"))?;
        Ok(Self::from_metadata(
            Schema::from_arrow_schema(arrow_schema.as_ref()),
            metadata,
            file_bytes,
            widths,
        ))
    }

    /// The rows in the file.
    pub fn rows(&self) -> usize {
        self.row_group_rows.iter().sum()
    }
}

/// Uncompressed bytes of each column in a footer, summed over row groups and nested
/// leaves: the only way to know a binary or string column's size short of reading it.
pub fn parquet_column_bytes(
    schema: &Schema,
    metadata: &polars_parquet::parquet::metadata::FileMetadata,
) -> Vec<(String, usize)> {
    schema
        .iter_names()
        .map(|name| {
            let bytes: i64 = metadata
                .row_groups
                .iter()
                .flat_map(|rg| rg.columns_under_root_iter(name).into_iter().flatten())
                .map(|chunk| chunk.uncompressed_size())
                .sum();
            (name.to_string(), bytes.max(0) as usize)
        })
        .collect()
}

/// Uncompressed bytes per row of each column over the footers read; files without a
/// column count its rows as zero (they read null).
pub fn column_bytes_per_row(footers: &[Option<FileFooter>]) -> Vec<(String, usize)> {
    let rows: usize = footers.iter().flatten().map(FileFooter::rows).sum();
    if rows == 0 {
        return Vec::new();
    }
    let mut totals: Vec<(String, usize)> = Vec::new();
    let mut at: HashMap<String, usize> = HashMap::new();
    for (name, bytes) in footers.iter().flatten().flat_map(|f| &f.column_bytes) {
        match at.get(name) {
            Some(&i) => totals[i].1 += bytes,
            None => {
                at.insert(name.clone(), totals.len());
                totals.push((name.clone(), *bytes));
            }
        }
    }
    totals
        .into_iter()
        .map(|(name, bytes)| (name, bytes / rows))
        .collect()
}

/// Where a dataset's schema came from, shown in Info's Schema tab so a missing column
/// traces to the files looked at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaOrigin {
    /// Every file's footer was read.
    AllFooters(usize),
    /// Too many files to read every footer: a sample spread evenly across them.
    FooterSample { read: usize, total: usize },
}

impl SchemaOrigin {
    /// How many files the dataset has, read or not; [`DatasetSchema::files`] is how many
    /// footers were read.
    pub fn total_files(&self) -> usize {
        match self {
            SchemaOrigin::AllFooters(files) => *files,
            SchemaOrigin::FooterSample { total, .. } => *total,
        }
    }
}

impl std::fmt::Display for SchemaOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaOrigin::AllFooters(1) => write!(f, "one footer"),
            SchemaOrigin::AllFooters(n) => {
                write!(f, "all {} footers", crate::numfmt::group_chrome(*n))
            }
            SchemaOrigin::FooterSample { read, total } => write!(
                f,
                "{} of {} footers (sample)",
                crate::numfmt::group_chrome(*read),
                crate::numfmt::group_chrome(*total)
            ),
        }
    }
}

/// What the footers said about one column.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDrift {
    pub name: PlSmallStr,
    /// The type the scan reads the column as.
    pub dtype: DataType,
    /// Files that have the column at all.
    pub present_in: usize,
    /// Files whose type does not fit `dtype`. The column is not read from them.
    pub conflicting_files: usize,
    /// The types those files use, in the order they first appear.
    pub conflicting_types: Vec<DataType>,
    /// Some file stores the column in a narrower type than `dtype`.
    pub widened: bool,
}

impl ColumnDrift {
    /// The column is in every file that was read, in one type.
    pub fn is_uniform(&self, files: usize) -> bool {
        self.present_in == files && self.conflicting_files == 0 && !self.widened
    }
}

/// Where a column not in every file sits among the dataset's partitions: in one
/// partition, or from some point onward (a field added to a feed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnRange {
    /// Every file that has it is under this one partition.
    Only(String),
    /// No file before this partition has it, and every file from there on does.
    NoneBefore(String),
}

/// Files a dataset's listing walked past. A file beside the data (a `.csv` beside
/// Parquet) may have been meant as data and is worth saying; one elsewhere (Delta's
/// `_delta_log/`, Hudi's `.hoodie/`, Iceberg's `metadata/`, folder markers) is
/// infrastructure: a directory with no Parquet is nobody's table. Counts only what
/// the walk saw.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SkippedFiles {
    /// Files a writer leaves beside the data: a name beginning `_` or `.`.
    pub bookkeeping: usize,
    /// Everything else that is not a Parquet file.
    pub not_parquet: usize,
    /// Empty objects whose names say data: likely a write that stopped, the skip most
    /// worth saying. Sizes come from the store listing, a stat, or a failed footer read.
    pub empty: usize,
}

impl SkippedFiles {
    /// Count one passed-over file. `bookkeeping` is the caller's call: only the location
    /// tells a stray `.json` from a lake table's record.
    pub fn count(&mut self, bookkeeping: bool) {
        if bookkeeping {
            self.bookkeeping += 1;
        } else {
            self.not_parquet += 1;
        }
    }
}

/// The reader settings that change what a sample of a file's columns returns, taken
/// from the open's actual options (`from_args_and_config` always fills
/// `infer_schema_length` and `parse_strings`, so guessing from "did the user set
/// anything" never works). [`Default`] matches Polars' readers, as home opens use.
#[derive(Debug, Clone)]
pub struct ReadAs {
    pub delimiter: Option<u8>,
    pub has_header: Option<bool>,
    pub skip_rows: Option<usize>,
    pub skip_lines: Option<usize>,
    pub infer_schema_length: Option<usize>,
    pub ignore_errors: bool,
    pub try_parse_dates: bool,
    pub comment_char: Option<String>,
    pub header_rows: Vec<usize>,
    pub header_join: String,
}

impl ReadAs {
    /// The open's options for the same settings, so the sample uses the open's reader
    /// setup.
    fn open_options(&self, format: crate::FileFormat) -> crate::OpenOptions {
        crate::OpenOptions {
            delimiter: self.delimiter.or(format.separator()),
            has_header: self.has_header,
            skip_rows: self.skip_rows,
            skip_lines: self.skip_lines,
            infer_schema_length: self.infer_schema_length,
            ignore_errors: self.ignore_errors,
            parse_dates: self.try_parse_dates,
            parse_strings: None,
            comment_char: self.comment_char.clone(),
            header_rows: self.header_rows.clone(),
            header_join: self.header_join.clone(),
            ..crate::OpenOptions::default()
        }
    }
}

impl Default for ReadAs {
    /// What a home-opened directory reads with: `OpenOptions::default()` plus `hive`,
    /// where `csv_try_parse_dates()` is true since `parse_strings` is unset. Written out:
    /// a derived default would give `try_parse_dates: false`.
    fn default() -> Self {
        Self {
            delimiter: None,
            has_header: None,
            skip_rows: None,
            skip_lines: None,
            infer_schema_length: None,
            ignore_errors: false,
            try_parse_dates: true,
            comment_char: None,
            header_rows: Vec::new(),
            header_join: crate::csv_dialect::DEFAULT_HEADER_JOIN.to_string(),
        }
    }
}

/// The column names one data file holds, read as cheaply as its format allows: the
/// header or first object's keys, via Polars' inference as the open will. For
/// formats without footers ([`is_nested`]'s evidence). `None` when the schema needs
/// the whole file (JSON documents) or the file will not parse: no evidence, so the
/// directory keeps its name-based kind.
pub fn column_schema_of(
    path: &std::path::Path,
    format: crate::FileFormat,
    as_read: &ReadAs,
) -> Option<Vec<(String, DataType)>> {
    use polars::prelude::{LazyFileListReader, LazyJsonLineReader};
    let lf = match format.descriptor().lines {
        Some(crate::cli::Lines::Delimited(_)) => {
            // Read as the open will: header placement and inference length change the result.
            let options = as_read.open_options(format);
            let header = crate::readers::csv::csv_header_names_of(&options, path, None).ok()?;
            let reader = crate::readers::csv::configure_csv_reader(
                crate::readers::csv::csv_reader_of(path).ok()?,
                &options,
                None,
            );
            crate::csv_dialect::name_columns(reader.finish().ok()?, header.as_deref()).ok()?
        }
        Some(crate::cli::Lines::Json) => {
            LazyJsonLineReader::new(crate::source::polars_literal_path(path).ok()?)
                .finish()
                .ok()?
        }
        // Every file of lines has the same two columns.
        Some(crate::cli::Lines::Text) => {
            return Some(
                crate::lines::schema(false)
                    .iter()
                    .map(|(name, dtype)| (name.to_string(), dtype.clone()))
                    .collect(),
            );
        }
        None => return None,
    };
    let schema = lf.clone().collect_schema().ok()?;
    // Trimmed, as `trim_csv_column_names` trims the read: `id, name` and `id,name` are the
    // same columns on screen.
    let fields: Vec<(String, DataType)> = schema
        .iter()
        .map(|(name, dtype)| (name.trim().to_string(), dtype.clone()))
        .collect();
    Some(fields)
}

/// Whether a schema is an empty file's: no columns, or one unnamed column (a header
/// written on a day with no rows reads as `""`). One unnamed column nests inside any
/// schema and would make any directory one table; inside a wider schema it is an
/// ordinary written-out index.
fn is_an_empty_file(schema: &[(String, DataType)]) -> bool {
    match schema {
        [] => true,
        [(only, _)] => only.trim().is_empty(),
        _ => false,
    }
}

/// Whether what came back are column names at all, or the first row of a file that has
/// no header.
///
/// datui reads a CSV as having a header, so a headerless one gives its first row of
/// *data* as the names: two files of the same table come back `["1", "2"]` and
/// `["5", "6"]` and look like separate tables, and those values would go on to the home
/// screen's column index as if they were column names.
///
/// Polars cannot tell the two apart either, and neither can anyone: a first row reading
/// `alice,30` is a header or it is not, and only the file knows. What is decidable is the
/// case that matters — every name a number — because a header of nothing but numbers is
/// vanishingly rare and a row of them is the common headerless shape. Where it fires the
/// answer is "no evidence", which leaves the directory as its names suggested and the
/// read to union what it finds.
pub(crate) fn names_are_names(names: &[String]) -> bool {
    !names.is_empty() && !names.iter().all(|n| n.trim().parse::<f64>().is_ok())
}

/// What a spread of a directory's files says about whether they are one table.
///
/// One sampling, read once, answering both questions asked of it: whether the files
/// nest, which decides what `Enter` does, and whether they agree, which decides what
/// the dataset says about itself. Taking them separately meant reading the same three
/// files twice and, worse, letting the two answers disagree.
#[derive(Debug, Clone, Default)]
pub struct Sampled {
    /// Every column name any sampled file has, first seen first. Empty when nothing
    /// could be read.
    pub columns: Vec<String>,
    /// Whether every sampled file's columns are within the widest one's. `None` where
    /// fewer than two files could be read, which decides nothing.
    pub nests: Option<bool>,
    /// Whether a column one file has is missing from another. The table is then their
    /// union, and a row from a file without the column reads null.
    pub columns_differ: bool,
    /// Whether a shared column has two types (`amount` Int64 in one file, String where a
    /// row said `N/A`): the read silently widens it for the whole directory otherwise.
    pub types_differ: bool,
    /// How many files were read, so a count over them is known exact or a floor.
    pub read: usize,
    /// Whether the files seem headerless, so the "names" are each file's first data row.
    /// Such a directory needs `--no-header` to read as one table; this replaces any
    /// nesting verdict or column note.
    pub headerless: bool,
}

impl Sampled {
    /// How the files differ, for the note that says so.
    pub fn disagreement(&self) -> Disagreement {
        // Headerless: the other two would describe column names that are really data.
        if self.headerless {
            return Disagreement {
                headerless: true,
                ..Default::default()
            };
        }
        Disagreement {
            columns: self.columns_differ,
            types: self.types_differ,
            headerless: false,
        }
    }
}

/// How a directory's files differed, as read: a column some files lack (ordinary
/// drift), and a column held in two types (forcing the wider). Either, both or
/// neither.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Disagreement {
    pub columns: bool,
    pub types: bool,
    /// See [`Sampled::headerless`].
    pub headerless: bool,
}

impl Disagreement {
    pub fn any(&self) -> bool {
        self.columns || self.types || self.headerless
    }
}

/// Read a spread of `files` (the ends and the middle, since sorted names group each
/// table's files) and say what they are. Bounded reads whatever the size: this runs
/// in listing passes and on the way into an open.
pub fn sample_files(
    files: &[std::path::PathBuf],
    format: crate::FileFormat,
    as_read: &ReadAs,
) -> Sampled {
    // Enough files to see a disagreement, with a bound on tries: daily directories have
    // empty days, and a spread landing on them learns nothing.
    const WANTED: usize = 3;
    const TRIES: usize = 12;
    let last = files.len().saturating_sub(1);
    // Three anchors (the ends and the middle) land in different runs of a table-by-table
    // directory; evenly spread reads would cluster in one run and agree. From each
    // anchor, step past empty files to a neighbor in the same run.
    const NEAR: usize = 4;
    let anchors = [0usize, last / 2, last];

    let mut read: Vec<Vec<(String, DataType)>> = Vec::new();
    let mut tried = 0usize;
    let mut seen: Vec<usize> = Vec::new();
    'anchors: for anchor in anchors {
        for step in 0..NEAR {
            if read.len() >= WANTED || tried >= TRIES {
                break 'anchors;
            }
            let i = anchor + step;
            if i > last || seen.contains(&i) {
                continue;
            }
            seen.push(i);
            let Some(file) = files.get(i) else { continue };
            tried += 1;
            // An empty file has no columns and is no evidence; ask its neighbor.
            if let Some(schema) = column_schema_of(file, format, as_read)
                && !is_an_empty_file(&schema)
            {
                read.push(schema);
                continue 'anchors;
            }
        }
    }

    let mut out = Sampled {
        read: read.len(),
        ..Default::default()
    };
    for file in &read {
        for (name, _) in file {
            if !out.columns.iter().any(|c| c == name) {
                out.columns.push(name.clone());
            }
        }
    }
    if read.len() < 2 {
        return out;
    }
    let names: Vec<Vec<String>> = read
        .iter()
        .map(|f| f.iter().map(|(n, _)| n.clone()).collect())
        .collect();
    // First-row-as-names: no nesting verdict or column note is meaningful without
    // `--no-header`. `nests` is `Some(false)` so `Enter` steps inside instead of building
    // a sheet of nulls, and the "columns" (`4`, `7`) stay out of the search index.
    if names.iter().any(|f| !names_are_names(f)) {
        out.columns.clear();
        out.headerless = true;
        out.nests = Some(false);
        return out;
    }
    let nests = is_nested(&names);
    out.nests = Some(nests);
    // A column two files hold in two types is a disagreement the names cannot show.
    let mut types: HashMap<&str, &DataType> = HashMap::new();
    let mut typed_apart = false;
    for (name, dtype) in read.iter().flatten() {
        match types.get(name.as_str()) {
            Some(seen) if *seen != dtype => typed_apart = true,
            Some(_) => {}
            None => {
                types.insert(name.as_str(), dtype);
            }
        }
    }
    // Not `!nests`: nested files can still miss a column, the ordinary drift the note is
    // for.
    let widest = names.iter().map(|f| f.len()).max().unwrap_or(0);
    out.columns_differ = names.iter().any(|f| f.len() != widest) || !nests;
    out.types_differ = typed_apart;
    out
}

/// Whether every file's columns are contained in the widest file's: the shape schema
/// evolution produces, read cleanly as a union. No score or threshold: a file either
/// brings a column no other has or not (overlap ratios cannot tell drift from
/// unrelated tables sharing a key). Failing is not a refusal: the row goes inside,
/// and `(all files)` still opens the union. Fewer than two files, or files without
/// columns, are one table.
pub fn is_nested(files: &[Vec<String>]) -> bool {
    let Some(widest) = files.iter().max_by_key(|f| f.len()) else {
        return true;
    };
    let widest: std::collections::BTreeSet<&str> = widest.iter().map(String::as_str).collect();
    files
        .iter()
        .all(|file| file.iter().all(|name| widest.contains(name.as_str())))
}

/// The top-level column names of Parquet leaf paths, in order, deduplicated. Leaf
/// wrappers (`inputs.list.element.address`) differ by writer, so comparisons use the
/// columns a reader sees.
pub fn top_level_columns(leaves: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    leaves
        .iter()
        .map(|leaf| leaf.split_once('.').map_or(leaf.as_str(), |(root, _)| root))
        .filter(|root| seen.insert(root.to_string()))
        .map(str::to_string)
        .collect()
}

/// One dataset's schema, and what deciding it revealed.
#[derive(Debug, Clone)]
pub struct DatasetSchema {
    pub schema: Arc<Schema>,
    pub columns: Vec<ColumnDrift>,
    /// Per file, the columns not read from it and the type it holds each in; empty when
    /// all fit. The type is the only way back to the values: reading the column as text
    /// reads each file at its own written type.
    pub omitted: Vec<Vec<(PlSmallStr, DataType)>>,
    /// Files whose footer could not be read, by index into the files given.
    pub unreadable: Vec<usize>,
    /// Files whose footer was read, readable or not.
    pub files: usize,
    /// The distinct ways files differ from the schema; group 0 is "nothing missing", as
    /// an unread file counts.
    pub groups: Vec<DriftGroup>,
    /// Per file, in the order given, its group in `groups`.
    pub file_group: Vec<u32>,
    pub origin: SchemaOrigin,
    /// Columns read as text from every file instead of the majority type; empty as the
    /// footers found it.
    pub read_as_text: Vec<PlSmallStr>,
    /// Files whose footer said they hold no rows, among those read.
    pub empty_files: usize,
    /// The median row group's compressed size over all footers read; `None` if none
    /// reported one.
    pub median_row_group_bytes: Option<usize>,
    /// The middle file's size, over the footers read. `None` when none was read.
    pub median_file_bytes: Option<usize>,
    /// Where each column not in every file sits, when its files form a nameable shape.
    /// Empty without partitions, or when footers were sampled (an unread file looks
    /// complete).
    pub column_ranges: HashMap<PlSmallStr, ColumnRange>,
    /// The distinct partition layouts and their file counts, commonest first; usually
    /// one or none. From every file's name, which the listing knows and the footers do
    /// not.
    pub partition_layouts: Vec<(Vec<String>, usize)>,
    /// The layouts past those kept, and their files: not remembered in full, but counted.
    pub partition_layouts_dropped: (usize, usize),
    /// What the listing passed over on the way to the files read.
    pub skipped: SkippedFiles,
    /// File names read to find the layouts, including those without partition keys.
    pub listed_files: usize,
}

/// What a file is missing relative to the schema; files missing the same share a
/// group, so a row carries only its group.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct DriftGroup {
    /// Columns the file does not have. Their cells are absent, not null.
    pub absent: Vec<PlSmallStr>,
    /// Columns the file holds in a type the dataset's column cannot hold, so not read
    /// from it: conflicts, not nulls.
    pub unread: Vec<PlSmallStr>,
}

impl DriftGroup {
    pub fn is_empty(&self) -> bool {
        self.absent.is_empty() && self.unread.is_empty()
    }
}

impl DatasetSchema {
    /// Columns that are not in every file, or whose type had to give way.
    pub fn drifting(&self) -> impl Iterator<Item = &ColumnDrift> {
        let readable = self.files - self.unreadable.len();
        self.columns.iter().filter(move |c| !c.is_uniform(readable))
    }

    /// The dataset with its partition layouts counted from file names, keys taken below
    /// `root` (directories above the opened root are not in dispute). Keys compare as a
    /// set: Polars matches hive columns by name, so `y=1/m=1` and `m=2/y=2` agree.
    pub fn with_partition_layouts(mut self, root: &str, paths: &[String]) -> DatasetSchema {
        /// Layouts kept: the note names two and counts the rest, so a key per file need not
        /// hold half a million.
        const KEPT: usize = 64;
        // Keyed by spelling, not scanned: a layout per file must be slow, not quadratic.
        let mut counts: HashMap<Vec<String>, usize> = HashMap::new();
        for path in paths {
            // A path outside the root cannot be placed; scanning it whole would count keys above
            // the dataset.
            let Some(below) = path.strip_prefix(root) else {
                continue;
            };
            let keys = partition_keys_of(below);
            if keys.is_empty() {
                continue;
            }
            *counts.entry(keys).or_insert(0) += 1;
        }
        let mut counts: Vec<(Vec<String>, usize)> = counts.into_iter().collect();
        // Commonest first, ties by key, so the same dataset names the same layout every open.
        counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        // Counted before dropping, so "and N other ways" counts them all.
        let dropped = &counts[counts.len().min(KEPT)..];
        self.partition_layouts_dropped =
            (dropped.len(), dropped.iter().map(|(_, files)| files).sum());
        counts.truncate(KEPT);
        self.partition_layouts = counts;
        self.listed_files = paths.len();
        self.column_ranges = self.ranges_of_columns(root, paths);
        self
    }

    /// Where each column not in every file sits, by partition (see [`ColumnRange`]).
    /// Nothing for a sampled dataset.
    fn ranges_of_columns(&self, root: &str, paths: &[String]) -> HashMap<PlSmallStr, ColumnRange> {
        // An unread footer (sampled or unparsable) records "missing nothing", right for
        // drawing cells but wrong for saying where a column begins.
        if matches!(self.origin, SchemaOrigin::FooterSample { .. })
            || !self.unreadable.is_empty()
            || self.file_group.len() != paths.len()
        {
            return HashMap::new();
        }
        // Per column: the first file having it, the last lacking it, and the partitions of
        // files having it (two suffice to rule out "only").
        struct Seen {
            first_present: Option<usize>,
            last_absent: Option<usize>,
            /// Partitions of files with it (two suffice: then it is not "only" anywhere) and
            /// without it (a sample: missing one costs a note, never a wrong one).
            with: Vec<String>,
            without: Vec<String>,
            /// A file with it sits under no partition (at the root): no "all under" claim is
            /// possible.
            unplaced: bool,
        }
        let partition_of = |index: usize| -> Option<String> {
            let below = paths.get(index)?.strip_prefix(root)?;
            let values = partition_values_of(below);
            (!values.is_empty()).then(|| values.join("/"))
        };
        // Whether listing order matches reading order: bytewise sort puts `part=10` before
        // `part=2`, and then no start can be named.
        let reads_in_order = (0..paths.len())
            .filter_map(&partition_of)
            .collect::<Vec<_>>()
            .windows(2)
            .all(|pair| natural_cmp(&pair[0], &pair[1]) != std::cmp::Ordering::Greater);
        // The drifting columns, and each group's lacking ones, settled once: few groups,
        // many files.
        let drifting: Vec<&ColumnDrift> = self
            .columns
            .iter()
            .filter(|column| column.present_in > 0 && column.present_in < self.files)
            .collect();
        if drifting.is_empty() {
            return HashMap::new();
        }
        // Built from each group's absent list, not by asking every column (a group per file
        // is possible).
        let where_in_drifting: HashMap<&PlSmallStr, usize> = drifting
            .iter()
            .enumerate()
            .map(|(at, column)| (&column.name, at))
            .collect();
        let missing_by_group: Vec<Vec<bool>> = self
            .groups
            .iter()
            .map(|group| {
                let mut missing = vec![false; drifting.len()];
                for name in &group.absent {
                    if let Some(at) = where_in_drifting.get(name) {
                        missing[*at] = true;
                    }
                }
                missing
            })
            .collect();
        let none_missing: Vec<bool> = vec![false; drifting.len()];
        // By index, not name: hashing names per file per column dominated the cost.
        let mut seen: Vec<Seen> = (0..drifting.len())
            .map(|_| Seen {
                first_present: None,
                last_absent: None,
                with: Vec::new(),
                without: Vec::new(),
                unplaced: false,
            })
            .collect();

        for (index, group) in self.file_group.iter().enumerate() {
            let missing: &[bool] = missing_by_group
                .get(*group as usize)
                .map(Vec::as_slice)
                .unwrap_or(&none_missing);
            // Split once per file rather than once per file and column.
            let here = partition_of(index);
            for (at, absent) in missing.iter().enumerate() {
                let entry = &mut seen[at];
                let seen_of = if *absent {
                    entry.last_absent = Some(index);
                    &mut entry.without
                } else {
                    entry.first_present.get_or_insert(index);
                    entry.unplaced |= here.is_none();
                    &mut entry.with
                };
                if seen_of.len() < 2
                    && let Some(partition) = here.as_ref()
                    && !seen_of.contains(partition)
                {
                    seen_of.push(partition.clone());
                }
            }
        }
        seen.into_iter()
            .zip(&drifting)
            .filter_map(|(entry, column)| {
                let name = column.name.clone();
                let first = entry.first_present?;
                // One partition holds every file with it, and some file without it is elsewhere, or
                // "only" says nothing.
                if !entry.unplaced
                    && entry.with.len() == 1
                    // Elsewhere as a place, not a string: `y=2024/m=03` is under `y=2024`.
                    && entry
                        .without
                        .iter()
                        .any(|other| {
                            !partition_holds(&entry.with[0], other)
                                && !same_place(&entry.with[0], other)
                        })
                {
                    return Some((name, ColumnRange::Only(entry.with[0].clone())));
                }
                // Every file without it comes before every file with it, checked against partition
                // values by the reader.
                let last_absent = entry.last_absent?;
                // `unplaced` is not checked here: "none before X" is about order, which an
                // unpartitioned file after the boundary does not contradict.
                if last_absent > first || !reads_in_order {
                    return None;
                }
                let (ends, begins) = (partition_of(last_absent)?, partition_of(first)?);
                // A real boundary: not the same place spelled differently, nor one directory inside
                // the other.
                (!same_place(&ends, &begins)
                    && !partition_holds(&begins, &ends)
                    && !partition_holds(&ends, &begins))
                .then_some((name, ColumnRange::NoneBefore(begins)))
            })
            .collect()
    }

    /// Record what the listing passed over. See [`SkippedFiles`].
    pub fn with_skipped(mut self, skipped: SkippedFiles) -> DatasetSchema {
        self.skipped = skipped;
        self
    }

    /// This dataset as it reads with `as_text` read as text from every file: for the
    /// panel and the table, not the scan (which needs the footer types, so `omitted` is
    /// kept and the caller keeps the original). Those columns stop conflicting; `absent`
    /// stays (a file without the column still shows none).
    pub fn reading_as_text(&self, as_text: &[PlSmallStr]) -> DatasetSchema {
        let mut out = self.clone();
        if as_text.is_empty() {
            return out;
        }
        out.schema = crate::schema_union::text_schema(&self.schema, as_text);
        for column in &mut out.columns {
            if as_text.contains(&column.name) {
                column.dtype = DataType::String;
                column.conflicting_files = 0;
                column.conflicting_types.clear();
                // `widened` stays: a widened value still reads at the column's type (`7.0`), so its
                // note still explains it.
            }
        }
        for group in &mut out.groups {
            group.unread.retain(|name| !as_text.contains(name));
        }
        out.read_as_text = as_text.to_vec();
        out
    }

    /// Whether any file is missing anything; if not, the scan is plain and rows carry
    /// nothing.
    pub fn drifts(&self) -> bool {
        self.groups.iter().any(|g| !g.is_empty())
    }
}

/// Footers read at once: one wave. Reads wait on round trips rather than compute, so
/// this exceeds the core count. Larger datasets open from their ends and read the
/// rest behind.
pub const FOOTERS_AT_ONCE: usize = 64;

/// Footers as the shape cache keeps them: schemas tabled and referenced by index,
/// since ten thousand files usually share one schema.
pub fn footers_to_cache(
    footers: &[Option<FileFooter>],
) -> (
    Vec<crate::cache::CachedFooter>,
    Vec<Vec<(String, DataType)>>,
) {
    let mut schemas = Vec::new();
    let cached = footers
        .iter()
        .map(|footer| match footer {
            None => crate::cache::CachedFooter::default(),
            Some(f) => crate::cache::CachedFooter {
                schema: Some(crate::cache::DatasetShape::intern_schema(
                    &mut schemas,
                    &f.schema,
                )),
                row_group_rows: f.row_group_rows.clone(),
                row_group_bytes: f.row_group_bytes.clone(),
                column_bytes: f.column_bytes.iter().map(|(_, bytes)| *bytes).collect(),
            },
        })
        .collect();
    (cached, schemas)
}

/// The cached footers as a fresh pass would read them, with `file_bytes` from the
/// listing. `None` for an unreadable footer, as the pass reports it. Refused whole
/// when inconsistent with itself or the listing.
pub fn footers_from_cache(
    cached: &[crate::cache::CachedFooter],
    schemas: &[Vec<(String, DataType)>],
    file_bytes: &[u64],
) -> Option<Vec<Option<FileFooter>>> {
    if cached.len() != file_bytes.len() {
        return None;
    }
    // One schema shared by every file that has it, as a fresh pass shares them.
    let shared: Vec<Arc<Schema>> = (0..schemas.len())
        .map(|at| crate::cache::DatasetShape::schema_at(schemas, at).map(Arc::new))
        .collect::<Option<_>>()?;
    cached
        .iter()
        .zip(file_bytes)
        .map(|(f, &bytes)| {
            let Some(at) = f.schema else {
                return Some(None);
            };
            let schema = shared.get(at)?;
            let column_bytes = schema
                .iter_names()
                .zip(&f.column_bytes)
                .map(|(name, bytes)| (name.to_string(), *bytes))
                .collect();
            Some(Some(FileFooter {
                schema: schema.clone(),
                row_group_rows: f.row_group_rows.clone(),
                row_group_bytes: f.row_group_bytes.clone(),
                file_bytes: bytes as usize,
                column_bytes,
            }))
        })
        .collect()
}

/// Something a test runs before each local footer read under a directory.
type FooterHook = Arc<dyn Fn(&std::path::Path) + Send + Sync>;

static FOOTER_HOOKS: std::sync::Mutex<Vec<(u64, std::path::PathBuf, FooterHook)>> =
    std::sync::Mutex::new(Vec::new());
/// Whether any hook is set, so a read with none takes no lock.
static FOOTER_HOOKS_SET: AtomicUsize = AtomicUsize::new(0);

/// Run `hook` before each local footer read under `dir` until the guard drops, for
/// tests counting or stalling reads; keyed by directory so parallel tests stay apart.
#[doc(hidden)]
pub fn on_local_footer_read(
    dir: &std::path::Path,
    hook: impl Fn(&std::path::Path) + Send + Sync + 'static,
) -> FooterHookGuard {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    FOOTER_HOOKS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push((id, dir.to_path_buf(), Arc::new(hook)));
    FOOTER_HOOKS_SET.fetch_add(1, Ordering::Release);
    FooterHookGuard(id)
}

/// Removes its hook when dropped.
#[doc(hidden)]
pub struct FooterHookGuard(u64);

impl Drop for FooterHookGuard {
    fn drop(&mut self) {
        FOOTER_HOOKS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(id, _, _)| *id != self.0);
        FOOTER_HOOKS_SET.fetch_sub(1, Ordering::Release);
    }
}

/// Run the hooks set for `path`'s directory, if any. Called by every local footer read.
pub(crate) fn before_local_footer_read(path: &std::path::Path) {
    if FOOTER_HOOKS_SET.load(Ordering::Acquire) == 0 {
        return;
    }
    // Cloned out so a hook that blocks does not hold the lock against the others.
    let hooks: Vec<FooterHook> = FOOTER_HOOKS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|(_, dir, _)| path.starts_with(dir))
        .map(|(_, _, hook)| hook.clone())
        .collect();
    for hook in hooks {
        hook(path);
    }
}

/// Footers read before a dataset opens; past this a spread sample stands in. A fixed
/// threshold documented in `docs/user-guide/large-datasets.md`.
pub const MAX_FOOTER_READS: usize = 20_000;

/// Which of a dataset's `files` footers to read: all of them, or — past
/// [`MAX_FOOTER_READS`] — a sample spread evenly across them, always including the
/// first and the newest. Indices are ascending.
pub fn footers_to_read(files: usize) -> Vec<usize> {
    if files <= MAX_FOOTER_READS {
        return (0..files).collect();
    }
    let last = files - 1;
    let mut sample: Vec<usize> = (0..MAX_FOOTER_READS)
        .map(|i| i * last / (MAX_FOOTER_READS - 1))
        .collect();
    sample.dedup();
    sample
}

/// The first and last files, which a dataset opens from while the rest of its footers
/// are read; ascending, one index for one file. Last by name, not date (unpadded
/// partition values sort oddly): a heuristic, with the rest arriving later.
pub fn ends_of(files: usize) -> Vec<usize> {
    match files {
        0 => Vec::new(),
        1 => vec![0],
        n => vec![0, n - 1],
    }
}

/// The schema of a dataset of `files` files whose footers at indices `read` were
/// fetched. The union is over the footers read; [`DatasetSchema::files`] stays that
/// count (datui knows nothing of unopened files). Only `omitted`, `file_group` and
/// `unreadable`, indexed by the scan, are spread to the full length.
pub fn union_sampled(
    files: usize,
    read: &[usize],
    footers: &[Option<FileFooter>],
) -> DatasetSchema {
    let origin = if read.len() == files {
        SchemaOrigin::AllFooters(files)
    } else {
        SchemaOrigin::FooterSample {
            read: read.len(),
            total: files,
        }
    };
    let mut union = union_file_schemas(footers, origin);
    let mut omitted = vec![Vec::new(); files];
    let mut file_group = vec![0u32; files];
    for ((columns, group), &index) in union.omitted.iter().zip(union.file_group.iter()).zip(read) {
        omitted[index] = columns.clone();
        file_group[index] = *group;
    }
    union.omitted = omitted;
    union.file_group = file_group;
    union.unreadable = union
        .unreadable
        .iter()
        .filter_map(|i| read.get(*i).copied())
        .collect();
    union
}

/// The paths whose footers were read. An unreadable footer is a file Polars cannot
/// read, and in the scan it would fail the first page for the whole dataset; it is
/// still counted for the note.
pub fn readable_paths<'a>(paths: &'a [String], unreadable: &[usize]) -> Cow<'a, [String]> {
    if unreadable.is_empty() {
        // Which is nearly always, and a dataset can be millions of paths.
        return Cow::Borrowed(paths);
    }
    // `unreadable` is ascending, so this is a search rather than a scan per path.
    debug_assert!(unreadable.windows(2).all(|pair| pair[0] < pair[1]));
    Cow::Owned(
        paths
            .iter()
            .enumerate()
            .filter(|(index, _)| unreadable.binary_search(index).is_err())
            .map(|(_, path)| path.clone())
            .collect(),
    )
}

/// Fold every file's footer into one schema. `files` is in scan order (the last
/// readable is the newest); `None` is an unreadable footer.
pub fn union_file_schemas(files: &[Option<FileFooter>], origin: SchemaOrigin) -> DatasetSchema {
    let unreadable = files
        .iter()
        .enumerate()
        .filter_map(|(i, f)| f.is_none().then_some(i))
        .collect();

    // Newest file first, in its own order, then whatever older files add.
    let mut order: Vec<PlSmallStr> = Vec::new();
    let mut seen: HashMap<PlSmallStr, usize> = HashMap::new();
    let mut push = |name: &PlSmallStr, order: &mut Vec<PlSmallStr>| {
        if !seen.contains_key(name) {
            seen.insert(name.clone(), order.len());
            order.push(name.clone());
        }
    };
    if let Some(newest) = files.iter().rev().flatten().next() {
        for name in newest.schema.iter_names() {
            push(name, &mut order);
        }
    }
    for file in files.iter().flatten() {
        for name in file.schema.iter_names() {
            push(name, &mut order);
        }
    }

    // Per column, every type a file gives it and the rows behind each.
    let mut sightings: Vec<Vec<(DataType, usize)>> = vec![Vec::new(); order.len()];
    for file in files.iter().flatten() {
        for (name, dtype) in file.schema.iter() {
            let Some(&index) = seen.get(name) else {
                continue;
            };
            sightings[index].push((dtype.clone(), file.rows()));
        }
    }

    let mut schema = Schema::with_capacity(order.len());
    let mut columns = Vec::with_capacity(order.len());
    for (name, seen_types) in order.iter().zip(sightings.iter()) {
        let chosen = choose_dtype(seen_types);
        let conflicting_types = seen_types
            .iter()
            .map(|(d, _)| d)
            .filter(|d| !fits(d, &chosen))
            .fold(Vec::new(), |mut acc: Vec<DataType>, d| {
                if !acc.contains(d) {
                    acc.push(d.clone());
                }
                acc
            });
        columns.push(ColumnDrift {
            name: name.clone(),
            present_in: seen_types.len(),
            conflicting_files: seen_types.iter().filter(|(d, _)| !fits(d, &chosen)).count(),
            widened: seen_types
                .iter()
                .any(|(d, _)| *d != chosen && fits(d, &chosen)),
            conflicting_types,
            dtype: chosen.clone(),
        });
        schema.with_column(name.clone(), chosen);
    }

    // What each file is missing, grouped; group 0 is "nothing missing", where unread
    // files fall.
    let mut groups: Vec<DriftGroup> = vec![DriftGroup::default()];
    let mut group_of: HashMap<DriftGroup, u32> = HashMap::from([(DriftGroup::default(), 0)]);
    let mut file_group = Vec::with_capacity(files.len());
    let mut omitted = Vec::with_capacity(files.len());
    for file in files {
        let Some(file) = file else {
            file_group.push(0);
            omitted.push(Vec::new());
            continue;
        };
        let stored: Vec<(PlSmallStr, DataType)> = file
            .schema
            .iter()
            .filter(|(name, dtype)| schema.get(name).is_some_and(|target| !fits(dtype, target)))
            .map(|(name, dtype)| (name.clone(), dtype.clone()))
            .collect();
        let unread: Vec<PlSmallStr> = stored.iter().map(|(name, _)| name.clone()).collect();
        let absent: Vec<PlSmallStr> = schema
            .iter_names()
            .filter(|name| !file.schema.contains(name))
            .cloned()
            .collect();
        omitted.push(stored);
        let group = DriftGroup { absent, unread };
        let next = groups.len() as u32;
        let id = *group_of.entry(group.clone()).or_insert_with(|| {
            groups.push(group);
            next
        });
        file_group.push(id);
    }

    DatasetSchema {
        schema: Arc::new(schema),
        columns,
        omitted,
        unreadable,
        files: files.len(),
        groups,
        file_group,
        origin,
        read_as_text: Vec::new(),
        empty_files: files.iter().flatten().filter(|f| f.rows() == 0).count(),
        median_file_bytes: median(files.iter().flatten().map(|f| f.file_bytes)),
        column_ranges: HashMap::new(),
        skipped: SkippedFiles::default(),
        partition_layouts: Vec::new(),
        partition_layouts_dropped: (0, 0),
        listed_files: 0,
        median_row_group_bytes: median(
            files
                .iter()
                .flatten()
                .flat_map(|f| f.row_group_bytes.iter().copied()),
        ),
    }
}

/// Whether one partition path holds another, as places (`y=2024` holds
/// `y=2024/m=03`), so "only y=2024" is false if a file without the column sits in
/// `y=2024/m=03`.
fn partition_holds(outer: &str, inner: &str) -> bool {
    inner == outer
        || inner
            .strip_prefix(outer)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// Whether two partition paths name one place however written: `m=03` is `m=3`, and
/// key order does not matter (hive matches by name).
fn same_place(a: &str, b: &str) -> bool {
    fn sorted(path: &str) -> Vec<&str> {
        let mut segments: Vec<&str> = path.split('/').collect();
        segments.sort_by_key(|segment| segment.split_once('=').map(|(key, _)| key));
        segments
    }
    let (a, b) = (sorted(a), sorted(b));
    a.len() == b.len()
        && a.iter()
            .zip(&b)
            .all(|(x, y)| natural_cmp(x, y) == std::cmp::Ordering::Equal)
}

/// Compares partition values as a reader does (`part=2` before `part=10`), where the
/// bytewise listing does not; see [`ends_of`].
fn natural_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let (mut a, mut b) = (a.as_bytes(), b.as_bytes());
    loop {
        match (a.first(), b.first()) {
            (None, None) => return Ordering::Equal,
            (None, _) => return Ordering::Less,
            (_, None) => return Ordering::Greater,
            (Some(x), Some(y)) if x.is_ascii_digit() && y.is_ascii_digit() => {
                let digits = |s: &[u8]| s.iter().take_while(|c| c.is_ascii_digit()).count();
                let (na, nb) = (digits(a), digits(b));
                // Leading zeros do not change a number: `m=03` and `m=3` are one month.
                let (xs, ys) = (&a[..na], &b[..nb]);
                fn trim(s: &[u8]) -> &[u8] {
                    let lead = s.iter().take_while(|c| **c == b'0').count();
                    &s[lead.min(s.len().saturating_sub(1))..]
                }
                let (tx, ty) = (trim(xs), trim(ys));
                match tx.len().cmp(&ty.len()).then_with(|| tx.cmp(ty)) {
                    Ordering::Equal => {}
                    other => return other,
                }
                a = &a[na..];
                b = &b[nb..];
            }
            (Some(x), Some(y)) => match x.cmp(y) {
                Ordering::Equal => {
                    a = &a[1..];
                    b = &b[1..];
                }
                other => return other,
            },
        }
    }
}

/// The `key=value` segments of a path below the root, in written order, values
/// included: a partition is a place. Key order is not normalized here;
/// `same_place` handles places written twice.
fn partition_values_of(path: &str) -> Vec<String> {
    #[cfg(windows)]
    let separators: &[char] = &['/', '\\'];
    #[cfg(not(windows))]
    let separators: &[char] = &['/'];
    let mut segments: Vec<&str> = path.split(separators).collect();
    // The file name itself is not a partition, whatever it is called.
    segments.pop();
    segments
        .into_iter()
        .filter(|segment| {
            segment
                .split_once('=')
                .is_some_and(|(key, _)| !key.is_empty())
        })
        .map(|segment| segment.to_string())
        .collect()
}

/// The hive partition keys in a path, in order (`a=1/b=2/f.parquet` is `[a, b]`).
/// Only directory segments with a key count, never the file name.
fn partition_keys_of(path: &str) -> Vec<String> {
    let mut keys: Vec<String> = Vec::new();
    // A backslash separates only on Windows; on Linux it is a name character.
    #[cfg(windows)]
    let separators: &[char] = &['/', '\\'];
    #[cfg(not(windows))]
    let separators: &[char] = &['/'];
    let mut segments: Vec<&str> = path.split(separators).collect();
    segments.pop();
    for segment in segments {
        if let Some((key, _)) = segment.split_once('=')
            && !key.is_empty()
        {
            keys.push(key.to_string());
        }
    }
    // A set: hive matches by name, so orders do not differ; sorted and deduplicated.
    keys.sort();
    keys.dedup();
    keys
}

/// The middle value of `sizes` (the lower of two middles); `None` when empty. A
/// median, since one odd file would drag a mean to a size no row group has.
fn median(sizes: impl Iterator<Item = usize>) -> Option<usize> {
    let mut sizes: Vec<usize> = sizes.collect();
    if sizes.is_empty() {
        return None;
    }
    sizes.sort_unstable();
    Some(sizes[(sizes.len() - 1) / 2])
}

/// The type to read a column as, from each file's `(type, rows)`: the widest lossless
/// type when all fit, else the type behind the most rows (preferring one that covers
/// other files), ties to the first seen (the newest file's).
fn choose_dtype(seen: &[(DataType, usize)]) -> DataType {
    let mut distinct: Vec<DataType> = Vec::new();
    for (dtype, _) in seen {
        if !distinct.contains(dtype) {
            distinct.push(dtype.clone());
        }
    }
    match distinct.as_slice() {
        [] => return DataType::Null,
        [only] => return only.clone(),
        _ => {}
    }
    // A type no file has can still win (Int32 and Float32 read as Float64), so folds are
    // candidates too.
    let mut candidates = distinct.clone();
    for dtype in &distinct {
        let folded = distinct
            .iter()
            .filter(|other| widen(dtype, other).is_some())
            .try_fold(dtype.clone(), |acc, other| widen(&acc, other));
        if let Some(folded) = folded
            && !candidates.contains(&folded)
        {
            candidates.push(folded);
        }
    }
    let mut best: Option<(DataType, usize, usize)> = None;
    for candidate in candidates {
        let rows: usize = seen
            .iter()
            .filter(|(d, _)| fits(d, &candidate))
            .map(|(_, rows)| rows)
            .sum();
        let files = seen.iter().filter(|(d, _)| fits(d, &candidate)).count();
        let better = best
            .as_ref()
            .is_none_or(|(_, best_rows, best_files)| (rows, files) > (*best_rows, *best_files));
        if better {
            best = Some((candidate, rows, files));
        }
    }
    best.map(|(d, _, _)| d).unwrap_or(DataType::Null)
}

/// Whether a file storing `from` reads into a `to` column losslessly, matching
/// [`crate::cloud_hive::lenient_scan`]'s cast policy; otherwise the column is left
/// unread in that file.
pub fn fits(from: &DataType, to: &DataType) -> bool {
    widen(from, to).as_ref() == Some(to)
}

/// The narrowest type `a` and `b` both read into losslessly, if any.
pub fn widen(a: &DataType, b: &DataType) -> Option<DataType> {
    use DataType::*;
    if a == b {
        return Some(a.clone());
    }
    match (a, b) {
        (Null, other) | (other, Null) => Some(other.clone()),
        _ if a.is_integer() && b.is_integer() => widen_integers(a, b),
        _ if (a.is_integer() || a.is_float()) && (b.is_integer() || b.is_float()) => Some(Float64),
        (Datetime(a_unit, a_zone), Datetime(b_unit, b_zone)) if a_zone == b_zone => {
            Some(Datetime(finer_unit(*a_unit, *b_unit), a_zone.clone()))
        }
        (List(a_inner), List(b_inner)) => widen(a_inner, b_inner).map(|t| List(Box::new(t))),
        (Struct(a_fields), Struct(b_fields)) => widen_structs(a_fields, b_fields),
        _ => None,
    }
}

/// Integers widen to the larger; signed with unsigned need a type wide enough for
/// both, which `UInt64` never has.
fn widen_integers(a: &DataType, b: &DataType) -> Option<DataType> {
    use DataType::*;
    let signed = |d: &DataType| matches!(d, Int8 | Int16 | Int32 | Int64 | Int128);
    let bits = |d: &DataType| match d {
        Int8 | UInt8 => 8u32,
        Int16 | UInt16 => 16,
        Int32 | UInt32 => 32,
        Int64 | UInt64 => 64,
        _ => 128,
    };
    if signed(a) == signed(b) {
        let wider = if bits(a) >= bits(b) { a } else { b };
        return Some(wider.clone());
    }
    // Mixed: the unsigned values must fit in a signed type one step wider.
    let (unsigned, sgn) = if signed(a) { (b, a) } else { (a, b) };
    let needed = match bits(unsigned) {
        8 => Int16,
        16 => Int32,
        32 => Int64,
        64 => return None,
        _ => return None,
    };
    Some(if bits(sgn) >= bits(&needed) {
        sgn.clone()
    } else {
        needed
    })
}

/// A struct has every field of either side, shared fields widened; `a`'s order first,
/// so the newest file's layout leads.
fn widen_structs(a: &[Field], b: &[Field]) -> Option<DataType> {
    let mut fields: Vec<Field> = Vec::with_capacity(a.len() + b.len());
    for field in a {
        let widened = match b.iter().find(|other| other.name() == field.name()) {
            Some(other) => widen(field.dtype(), other.dtype())?,
            None => field.dtype().clone(),
        };
        fields.push(Field::new(field.name().clone(), widened));
    }
    for field in b {
        if !a.iter().any(|other| other.name() == field.name()) {
            fields.push(field.clone());
        }
    }
    Some(DataType::Struct(fields))
}

fn finer_unit(a: TimeUnit, b: TimeUnit) -> TimeUnit {
    let rank = |u: TimeUnit| match u {
        TimeUnit::Milliseconds => 0,
        TimeUnit::Microseconds => 1,
        TimeUnit::Nanoseconds => 2,
    };
    if rank(a) >= rank(b) { a } else { b }
}

/// Put the partition columns, typed from their directory names, ahead of the file's own.
pub fn with_partition_columns(
    file_schema: &Schema,
    partition_columns: &[String],
    values: &[(String, String)],
) -> Schema {
    let part_set: HashSet<&str> = partition_columns.iter().map(String::as_str).collect();
    let mut merged = Schema::with_capacity(partition_columns.len() + file_schema.len());
    for name in partition_columns {
        merged.with_column(
            name.clone().into(),
            crate::readers::hive::partition_dtype(name, file_schema, values),
        );
    }
    for (name, dtype) in file_schema.iter() {
        if !part_set.contains(name.as_str()) {
            merged.with_column(name.clone(), dtype.clone());
        }
    }
    merged
}

/// Partition column names from one file's path, in path order, each key once.
pub fn partition_columns_of_key(key: &str) -> Vec<String> {
    let mut columns = Vec::new();
    let mut seen = HashSet::new();
    for segment in key.split('/') {
        if let Some((name, _)) = segment.split_once('=')
            && !name.is_empty()
            && seen.insert(name.to_string())
        {
            columns.push(name.to_string());
        }
    }
    columns
}

/// A listed dataset's partition columns and typing values, from its first and newest
/// files' `/`-separated keys (the newest names the columns, as later keys appear
/// there). Shared by local and cloud listings so a tree reads the same.
pub fn partitions_of_listing(first: &str, newest: &str) -> (Vec<String>, Vec<(String, String)>) {
    let values = [first, newest]
        .iter()
        .flat_map(|key| key.split('/'))
        .filter_map(|segment| segment.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    (partition_columns_of_key(newest), values)
}

/// A dataset's row count from its footers, each read once: starting from those the
/// open read and reading the rest. Once all are in, the set is handed back once for
/// the shape cache, so a reopen reads none.
pub struct FooterCount<F> {
    files: usize,
    /// The files counted, as ascending indices: those the open could read.
    counted: Vec<usize>,
    /// Every file's footer, where read. Emptied once the count is whole.
    footers: std::sync::Mutex<Vec<Option<F>>>,
}

/// What a count found.
pub struct Counted<F> {
    /// The rows in each row group of each counted file, in order.
    pub row_groups: Vec<Vec<usize>>,
    /// Every file's footer, the first time all of them are in.
    pub whole: Option<Vec<Option<F>>>,
}

impl<F: Clone> FooterCount<F> {
    /// A count of `counted` among `files` files, starting from the footers in `known`, by
    /// index.
    pub fn new(
        files: usize,
        counted: Vec<usize>,
        known: impl IntoIterator<Item = (usize, Option<F>)>,
    ) -> Self {
        let mut footers = vec![None; files];
        for (index, footer) in known {
            if let Some(slot) = footers.get_mut(index) {
                *slot = footer;
            }
        }
        Self {
            files,
            counted,
            footers: std::sync::Mutex::new(footers),
        }
    }

    /// Count, reading missing footers with `read` (answering in asked order, `None` if
    /// abandoned). Earlier failures are retried, since a read can fail transiently. Holds
    /// the footers while reading so concurrent counts do not read the same ones.
    pub fn count(
        &self,
        read: impl FnOnce(&[usize]) -> Option<Vec<Option<F>>>,
        row_groups: impl Fn(&F) -> Vec<usize>,
    ) -> Option<Counted<F>> {
        let mut footers = self.footers.lock().unwrap_or_else(|e| e.into_inner());
        let missing = self.missing(&mut footers);
        let read = if missing.is_empty() {
            Vec::new()
        } else {
            read(&missing)?
        };
        Some(self.settle(&mut footers, missing, read, row_groups))
    }

    fn missing(&self, footers: &mut Vec<Option<F>>) -> Vec<usize> {
        if footers.is_empty() {
            // Already whole once; asked again, read again.
            *footers = vec![None; self.files];
        }
        self.counted
            .iter()
            .copied()
            .filter(|&index| footers[index].is_none())
            .collect()
    }

    fn settle(
        &self,
        footers: &mut Vec<Option<F>>,
        missing: Vec<usize>,
        read: Vec<Option<F>>,
        row_groups: impl Fn(&F) -> Vec<usize>,
    ) -> Counted<F> {
        if footers.is_empty() {
            *footers = vec![None; self.files];
        }
        for (index, footer) in missing.into_iter().zip(read) {
            footers[index] = footer;
        }
        let groups: Vec<Vec<usize>> = self
            .counted
            .iter()
            .map(|&index| footers[index].as_ref().map(&row_groups).unwrap_or_default())
            .collect();
        let whole = if footers.iter().all(Option::is_some) {
            Some(std::mem::take(footers))
        } else {
            if self.counted.iter().all(|&index| footers[index].is_some()) {
                // Counted, but a file the open could not read stays unread: nothing whole to keep.
                footers.clear();
            }
            None
        };
        Counted {
            row_groups: groups,
            whole,
        }
    }
}

/// The column the scan writes each row's dataset position into, tracing a cell to
/// its file and whether that file had the column. Never shown, filtered, sorted or
/// exported: the display projects only the column order.
pub const DRIFT_COLUMN: &str = "__datui_row";

/// A file that is missing nothing, for a group id with no entry of its own.
static NOTHING_MISSING: DriftGroup = DriftGroup {
    absent: Vec::new(),
    unread: Vec::new(),
};

/// How files differ, as the scan needs it: what each lacks and where its rows begin,
/// keyed by the path or URL the scan uses.
#[derive(Debug, Clone, Default)]
pub struct ScanDrift {
    group_of: HashMap<String, u32>,
    /// Where each file's rows start; a run numbers rows from its first file's entry, so a
    /// window of files still numbers right.
    row_of: HashMap<String, usize>,
    /// Per conflicting file, the type it holds each conflicting column in.
    stored_of: HashMap<String, Vec<(PlSmallStr, DataType)>>,
    pub groups: Vec<DriftGroup>,
}

impl ScanDrift {
    /// `None` when every file agrees or per-file row counts are unknown: the scan is then
    /// plain. `file_rows` is each file's row count, in `paths` order.
    pub fn new(paths: &[String], dataset: &DatasetSchema, file_rows: &[usize]) -> Option<Self> {
        if !dataset.drifts() || file_rows.len() != paths.len() {
            return None;
        }
        let group_of = paths
            .iter()
            .zip(dataset.file_group.iter())
            .filter(|(_, group)| **group != 0)
            .map(|(path, group)| (path.clone(), *group))
            .collect();
        let mut row = 0usize;
        let mut row_of = HashMap::with_capacity(paths.len());
        for (path, rows) in paths.iter().zip(file_rows) {
            row_of.insert(path.clone(), row);
            row += rows;
        }
        let stored_of = paths
            .iter()
            .zip(dataset.omitted.iter())
            .filter(|(_, stored)| !stored.is_empty())
            .map(|(path, stored)| (path.clone(), stored.clone()))
            .collect();
        Some(ScanDrift {
            group_of,
            row_of,
            stored_of,
            groups: dataset.groups.clone(),
        })
    }

    /// The group of the file the scan names `path`. Group 0 is "nothing missing".
    pub fn group(&self, path: &str) -> u32 {
        self.group_of.get(path).copied().unwrap_or(0)
    }

    /// Where the rows of the file the scan names `path` begin in the dataset.
    fn first_row(&self, path: &str) -> usize {
        self.row_of.get(path).copied().unwrap_or(0)
    }

    /// The type the file at `path` holds `column` in, when it differs from the read type.
    fn stored_type(&self, path: &str, column: &PlSmallStr) -> Option<&DataType> {
        self.stored_of
            .get(path)?
            .iter()
            .find(|(name, _)| name == column)
            .map(|(_, dtype)| dtype)
    }

    /// Which columns a file is not read for, which is the only reason to split the scan.
    fn unread(&self, path: &str) -> &[PlSmallStr] {
        self.groups
            .get(self.group(path) as usize)
            .unwrap_or(&NOTHING_MISSING)
            .unread
            .as_slice()
    }
}

/// A scan of `paths` into `schema` across files written at different times: missing
/// columns and fields fill with nulls, extras are ignored, and integers, floats and
/// datetime units widen (`scan_parquet` alone only fills).
///
/// With `drift`, each row carries its dataset position in [`DRIFT_COLUMN`], tracing
/// a cell to its file and telling a null from a column the file never had.
///
/// The scan splits only where a file stores a column in a conflicting type, which
/// must be left out of that file's read: consecutive files omitting the same columns
/// form one run, and runs concatenate in file order. Missing columns need no split.
///
/// `as_text` columns are read at each file's own type and cast to text after (Polars
/// cannot read a number as a string), so runs also split on those columns' stored
/// types. Widened or unknown-type files (no footer, outside a sample) read at the
/// column's type. Columns [`can_read_as_text`] refuses, or the schema lacks, are read
/// as before.
pub fn lenient_scan(
    paths: &[String],
    schema: Arc<Schema>,
    cloud_options: Option<polars::io::cloud::CloudOptions>,
    drift: Option<&ScanDrift>,
    as_text: &[PlSmallStr],
) -> PolarsResult<LazyFrame> {
    let Some(drift) = drift else {
        return scan_run(paths, &schema, cloud_options, &[], None, &[]);
    };
    // Only columns the schema has and every stored type of which casts to text; a
    // refused cast would fail the whole read.
    let as_text: Vec<PlSmallStr> = as_text
        .iter()
        .filter(|name| {
            schema.get(name).is_some_and(can_read_as_text)
                && paths
                    .iter()
                    .all(|path| drift.stored_type(path, name).is_none_or(can_read_as_text))
        })
        .cloned()
        .collect();
    let as_text = as_text.as_slice();
    // A file's read of an `as_text` column is not its omission, whatever its type.
    let unread_of = |path: &str| -> Vec<PlSmallStr> {
        drift
            .unread(path)
            .iter()
            .filter(|name| !as_text.contains(name))
            .cloned()
            .collect()
    };
    // A run agrees on what it leaves out and on each as-text column's stored type.
    let key_of = |path: &str| -> (Vec<PlSmallStr>, Vec<Option<DataType>>) {
        (
            unread_of(path),
            as_text
                .iter()
                .map(|name| drift.stored_type(path, name).cloned())
                .collect(),
        )
    };
    let mut runs: Vec<LazyFrame> = Vec::new();
    let mut start = 0;
    while start < paths.len() {
        let key = key_of(&paths[start]);
        let end = paths[start..]
            .iter()
            .position(|path| key_of(path) != key)
            .map_or(paths.len(), |offset| start + offset);
        let (omit, stored) = key;
        // The run reads each `as_text` column at the type its files wrote, then casts.
        let read_as: Vec<(PlSmallStr, DataType)> = as_text
            .iter()
            .zip(stored)
            .map(|(name, stored)| {
                let dtype = stored.or_else(|| schema.get(name).cloned());
                (name.clone(), dtype.unwrap_or(DataType::String))
            })
            .collect();
        runs.push(scan_run(
            &paths[start..end],
            &schema,
            cloud_options.clone(),
            &omit,
            Some(drift.first_row(&paths[start])),
            &read_as,
        )?);
        start = end;
    }
    match runs.len() {
        1 => Ok(runs.remove(0)),
        _ => concat(
            runs,
            UnionArgs {
                rechunk: false,
                parallel: true,
                ..Default::default()
            },
        ),
    }
}

/// Whether a column of this type can be shown as text. Polars cannot cast durations
/// or lists to strings, and binary fails on non-UTF-8, and a failed cast fails the
/// whole scan, so this is decided by type before offering.
/// `types_the_cast_agrees_with_are_exactly_the_ones_offered` checks it against
/// Polars.
pub fn can_read_as_text(dtype: &DataType) -> bool {
    match dtype {
        // Not text at all, and not convertible: the cast errors rather than escaping.
        DataType::Binary | DataType::BinaryOffset => false,
        DataType::Duration(_) => false,
        // Nested sequences have no string form in Polars 0.55.
        DataType::List(_) | DataType::Array(_, _) => false,
        // A struct prints itself (`{1,"a"}`), handling inner types a column could not cast.
        DataType::Struct(_) => true,
        DataType::Unknown(_) => false,
        _ => true,
    }
}

impl ColumnDrift {
    /// Whether this column can be read as text: every type any file holds it in, and the
    /// read type, must be castable.
    pub fn can_read_as_text(&self) -> bool {
        can_read_as_text(&self.dtype) && self.conflicting_types.iter().all(can_read_as_text)
    }
}

/// `schema` with every `as_text` column as text, for callers describing what they
/// hold ([`lenient_scan`] does not need it). Columns keep their places.
pub fn text_schema(schema: &Arc<Schema>, as_text: &[PlSmallStr]) -> Arc<Schema> {
    if as_text.is_empty() {
        return schema.clone();
    }
    let mut out = Schema::with_capacity(schema.len());
    for (name, dtype) in schema.iter() {
        let dtype = if as_text.contains(name) {
            DataType::String
        } else {
            dtype.clone()
        };
        out.with_column(name.clone(), dtype);
    }
    Arc::new(out)
}

/// One run of files missing the same things. `stamp` is the group written into
/// [`DRIFT_COLUMN`]; its presence also selects the dataset's column order so runs
/// concatenate.
fn scan_run(
    urls: &[String],
    schema: &Arc<Schema>,
    cloud_options: Option<polars::io::cloud::CloudOptions>,
    omit: &[PlSmallStr],
    first_row: Option<usize>,
    read_as: &[(PlSmallStr, DataType)],
) -> PolarsResult<LazyFrame> {
    use polars::lazy::dsl::{
        CastColumnsPolicy, DslBuilder, ExtraColumnsPolicy, MissingColumnsPolicy, ScanSources,
        UnifiedScanArgs,
    };
    use polars::prelude::{Expr, NULL, col, lit};
    let sources = ScanSources::Paths(
        urls.iter()
            .map(|url| PlRefPath::new(url.as_str()))
            .collect(),
    );
    let target = if omit.is_empty() && read_as.is_empty() {
        schema.clone()
    } else {
        let mut reduced = Schema::with_capacity(schema.len());
        for (name, dtype) in schema.iter() {
            if omit.contains(name) {
                continue;
            }
            // Read at the type these files wrote, not the display type.
            let dtype = read_as
                .iter()
                .find(|(column, _)| column == name)
                .map(|(_, dtype)| dtype)
                .unwrap_or(dtype);
            reduced.with_column(name.clone(), dtype.clone());
        }
        Arc::new(reduced)
    };
    let options = polars::io::parquet::read::ParquetOptions {
        schema: Some(target),
        ..Default::default()
    };
    let args = UnifiedScanArgs {
        cloud_options,
        hive_options: polars::io::HiveOptions::new_enabled(),
        glob: false,
        cast_columns_policy: CastColumnsPolicy {
            integer_upcast: true,
            integer_to_float_cast: true,
            float_upcast: true,
            datetime_nanoseconds_downcast: true,
            datetime_microseconds_downcast: true,
            datetime_milliseconds_upcast: true,
            datetime_microseconds_upcast: true,
            null_upcast: true,
            missing_struct_fields: MissingColumnsPolicy::Insert,
            extra_struct_fields: ExtraColumnsPolicy::Ignore,
            ..CastColumnsPolicy::ERROR_ON_MISMATCH
        },
        missing_columns_policy: MissingColumnsPolicy::Insert,
        extra_columns_policy: ExtraColumnsPolicy::Ignore,
        // Numbering rows from the run's start costs one column and no read, and survives
        // sorting.
        row_index: first_row.map(|first| polars::io::RowIndex {
            name: DRIFT_COLUMN.into(),
            offset: first as polars::prelude::IdxSize,
        }),
        ..Default::default()
    };
    let mut lf: LazyFrame = DslBuilder::scan_parquet(sources, options, args)?
        .build()
        .into();
    if !omit.is_empty() {
        let nulls: Vec<Expr> = omit
            .iter()
            .filter_map(|name| {
                let dtype = schema.get(name)?;
                Some(lit(NULL).cast(dtype.clone()).alias(name.clone()))
            })
            .collect();
        lf = lf.with_columns(nulls);
    }
    if !read_as.is_empty() {
        // Cast to text in every run so runs concatenate; a date past the calendar (which
        // panics Polars' cast) becomes its stored number.
        let texts: Vec<Expr> = read_as
            .iter()
            .map(|(name, _)| {
                crate::past_calendar::text_expr(col(name.clone()), CastOptions::NonStrict)
                    .alias(name.clone())
            })
            .collect();
        lf = lf.with_columns(texts);
    }
    if first_row.is_some() {
        // Runs concatenate only with the same column order; the row index arrives first, so
        // move it to the end where the state expects it.
        let mut ordered: Vec<Expr> = schema.iter_names().map(|name| col(name.clone())).collect();
        ordered.push(col(DRIFT_COLUMN));
        lf = lf.select(ordered);
    }
    Ok(lf)
}

#[cfg(test)]
mod tests;
