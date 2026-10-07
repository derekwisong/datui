//! The schema of a dataset made of many files.
//!
//! A dataset written over years is rarely uniform: a vendor adds a column one day,
//! drops another for a week, or writes a number as text for a month. Reading the
//! schema from one file makes those columns appear or vanish depending on which file
//! is picked. This module takes every column any file has, from the footers the row
//! count already reads, and decides one type per column:
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

/// A listing counting the objects it finds against a [`FooterProgress`], which stops
/// saying so however it ends.
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

/// What became of a footer pass, after it has finished saying so.
///
/// Hidden from the docs: nothing on screen reads it, and it is public only so the
/// wiring between an open and its counter can be checked from a test.
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

/// How far a dataset's footer pass has got, for the loading screen to read.
///
/// Opening a directory of many files reads a footer from each before a row is shown, and
/// on a few thousand files that is seconds of a screen that says only "Reading schema".
/// The count is what makes the wait legible: a number that climbs is a wait, and a
/// number that stops is a problem.
///
/// Shared with the threads doing the reading, which is why it is atomic and why it is
/// only ever written by them and read by the render.
#[derive(Debug, Default)]
pub struct FooterProgress {
    read: AtomicUsize,
    total: AtomicUsize,
    /// Passes begun. The count itself is unobservable once a pass has finished —
    /// read and total are both back to nothing — so without this there is no way to
    /// tell a pass that reported from one that never started.
    passes: AtomicUsize,
    /// What the last pass was over, kept after `done` for the same reason.
    last_total: AtomicUsize,
    /// The load this counter belongs to was abandoned: passes stop issuing
    /// reads. Shared out to the read tasks through [`Self::cancel_flag`].
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Objects a listing has found so far, while `listing` is set. A listing of a large
    /// prefix is the longest wait before any footer, and it has no total to count to.
    listed: std::sync::Arc<AtomicUsize>,
    listing: std::sync::atomic::AtomicBool,
    /// Footers read at once by a pass against this counter; `0` is [`FOOTERS_AT_ONCE`].
    at_once: AtomicUsize,
    /// The row count a sample of the footers says, once one is in.
    estimate: std::sync::Mutex<Option<RowEstimate>>,
}

/// A dataset's row count from a sample of its files' footers: the mean of the files
/// read, times the files there are. Said as `~4.12B rows (est.)` until it is counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowEstimate {
    pub rows: u64,
    /// Footers the mean is over.
    pub sampled: usize,
    /// Files in the dataset.
    pub files: usize,
}

impl RowEstimate {
    /// The estimate from the `footers` read of a dataset of `files` files; `None`
    /// when none read.
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

/// Footers sampled at random for a dataset's first row estimate: enough for a mean
/// within a few percent on any dataset whose files are alike, and one wave or a few.
pub const ESTIMATE_SAMPLE: usize = 2_000;

/// Footers read at once by a count: the exact count of a dataset of many files reads
/// every footer it does not have, and on a store each is a round trip of waiting.
pub const COUNT_AT_ONCE: usize = 256;

/// `n` of the indices below `files`, ascending, drawn at random from `seed`: every
/// index when there are no more than `n`. The same seed draws the same sample.
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
        // Released after the reset, and acquired in `reading`, so a render cannot pair
        // this pass's total with the last one's count and report a pass as finished at
        // the instant it starts.
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

    /// A pass over `total` footers that says it has finished however it ends.
    ///
    /// A panic between `begin` and `done` would otherwise leave the count on screen
    /// for as long as that counter is read — and the counter outlives the pass, since
    /// the render holds it.
    pub fn pass(&self, total: usize) -> Pass<'_> {
        self.begin(total);
        Pass(self)
    }

    /// What has become of the passes against this counter: how many have begun, how
    /// many footers the last one has read, and how many it was over.
    ///
    /// All three outlive the pass, which is the point of them. A finished pass reports
    /// nothing — read and total are both back to nothing — so from outside, a pass that
    /// counted and a pass that never started look identical, and every line that does
    /// the counting could be deleted with the tests green. Nothing on screen reads
    /// this; it is here so the wiring can be checked.
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

    /// The load was abandoned: any pass on this counter stops issuing reads.
    /// Cancelling is one-way; a new load gets a new counter.
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

/// What one file's footer said, short of the data: the one footer type, wherever the
/// file is and whichever pass read it.
#[derive(Debug, Clone)]
pub struct FileFooter {
    pub schema: Arc<Schema>,
    /// Rows in each row group, in file order.
    pub row_group_rows: Vec<usize>,
    /// Compressed bytes of each row group, in file order: what crosses the wire for
    /// that group, not what it occupies once decoded.
    ///
    /// A row group is the unit a reader fetches: a page of rows anywhere inside one
    /// costs the whole of it. How big they are is therefore what a remote dataset
    /// costs to scroll, and it is in the footer datui already reads.
    pub row_group_bytes: Vec<usize>,
    /// The file's size on disk or in the store.
    pub file_bytes: usize,
    /// Uncompressed bytes of each column, from [`parquet_column_bytes`]. Empty where
    /// the read did not keep them: a dataset in a store does not, so its remembered
    /// shape stays small.
    pub column_bytes: Vec<(String, usize)>,
}

impl FileFooter {
    /// What a footer's metadata says about a file of `file_bytes`, with each column's
    /// width where `widths` asks for it.
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

    /// The footer at the end of `tail`, which holds at least the file's last bytes up
    /// to and including its footer.
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

/// Uncompressed bytes of each column in a Parquet footer, summed over the row groups
/// and a nested column's leaves. What a binary or string column holds is known only
/// from here short of reading it.
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

/// Uncompressed bytes per row of each column over the footers read. A file without a
/// column counts its rows at nothing, since they read as null there.
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

/// Where a dataset's schema came from. Shown in the Info panel's Schema tab, so a
/// column that is missing is traceable to the files that were looked at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaOrigin {
    /// Every file's footer was read.
    AllFooters(usize),
    /// Too many files to read every footer: a sample spread evenly across them.
    FooterSample { read: usize, total: usize },
}

impl SchemaOrigin {
    /// How many files the dataset has, whether or not every footer was read.
    ///
    /// Not [`DatasetSchema::files`], which is how many footers were *read* — the
    /// population every other count in the notes is taken over. One note needs the
    /// other number, and the two are a keystroke apart, so this one says which.
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

/// Where a column that is not in every file sits, in the dataset's own partitions.
///
/// The count alone — "in 1 of 6,541 files" — says a column is unusual without saying
/// where to look. These are the two shapes worth naming: a column that belongs to one
/// partition, and one that starts partway through a dataset ordered by its partitions,
/// which is what a field added to a feed looks like ever after.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnRange {
    /// Every file that has it is under this one partition.
    Only(String),
    /// No file before this partition has it, and every file from there on does.
    NoneBefore(String),
}

/// What a dataset's listing walked past: files under the directory that are not read.
///
/// Split by whether anyone could have meant them as data, which is a question about
/// where a file is rather than what it is called.
///
/// **A file beside the data** — a `.csv` in a directory that also holds Parquet — is one
/// somebody may have expected in the table. That is worth saying.
///
/// **A file somewhere else** is infrastructure. Delta keeps its log in `_delta_log/`,
/// Hudi in `.hoodie/`, Iceberg in a plain `metadata/` beside the data; a bucket made
/// through a console is full of zero-byte folder markers. Naming those conventions one
/// by one is a game with no end — the test that holds for all of them is that a directory
/// with no Parquet in it is nobody's table, whatever it is called.
///
/// Not a complete accounting of the directory: a subtree that cannot be read, or one
/// below the depth the walk stops at, is neither listed nor counted. What the note says
/// is how many files were passed over among those it saw.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SkippedFiles {
    /// Files a writer leaves beside the data: a name beginning `_` or `.`.
    pub bookkeeping: usize,
    /// Everything else that is not a Parquet file.
    pub not_parquet: usize,
    /// Objects with nothing in them, whose name says they are data. Not a file anyone
    /// left on purpose: a write that stopped, which is the skip most worth saying.
    ///
    /// A store's listing knows each size; a directory knows it from the stat past one
    /// wave, or from the footer that would not read within one.
    pub empty: usize,
}

impl SkippedFiles {
    /// Count one file the listing passed over.
    ///
    /// `bookkeeping` is the caller's, because only the caller knows where the file was.
    /// A `.json` is a mistake beside the data and a record of the table inside
    /// `_delta_log/` or `metadata/`, and its own name cannot say which.
    pub fn count(&mut self, bookkeeping: bool) {
        if bookkeeping {
            self.bookkeeping += 1;
        } else {
            self.not_parquet += 1;
        }
    }
}

/// The few reader settings that change what a sample of a file's columns comes back as.
///
/// The sample exists to say what the *open* will do, so it reads each file the way the
/// open will. Guessing instead, and then standing down wherever the guess might be
/// wrong, does not work: `OpenOptions::from_args_and_config` fills in
/// `infer_schema_length` and `parse_strings` on every run with no flags at all, so a
/// predicate over "did the user set anything" is true every time and the sample never
/// runs. That shipped once — the notes about how a directory was stacked never appeared
/// outside the tests, which built `OpenOptions::default()` and saw `None` in both.
///
/// [`Default`] is what Polars' own readers do, which is what the home screen's opens
/// pass.
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
    /// The open's options that say the same, so the sample is configured by the
    /// open's own reader setup rather than a copy of it.
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
    /// What a directory opened from the home screen is read with:
    /// `OpenOptions::default()` plus `hive`, whose `csv_try_parse_dates()` is true
    /// because `parse_strings` is unset there.
    ///
    /// Written out rather than derived. A derived `Default` gives `try_parse_dates:
    /// false`, which is not what any caller wants and differs from what the read does —
    /// two defaults for one thing, and the wrong one reachable by anybody typing
    /// `ReadAs::default()`.
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

/// The column names one data file holds, read as cheaply as its format allows.
///
/// The evidence [`is_nested`] wants, for the formats that have no footer. Parquet's
/// answer comes from its footer and has its own path; this is for the rest, where the
/// names are at the front of the file — a CSV header line, an NDJSON object's keys —
/// and Polars' own schema inference is what reads them, so the names are the ones the
/// open will use, separator and all.
///
/// `None` for a format whose schema cannot be had without reading the whole file, and
/// for a file that would not parse. Both mean "no evidence", which every caller here
/// treats as it treats an unreadable footer: the directory keeps the kind its names
/// suggested, and the read that follows is lenient enough to survive being wrong.
///
/// Deliberately not JSON: a `.json` file is one document, and its keys are only known
/// once it has been parsed. Sampling three of those in a listing pass is a read of
/// three whole files for a label.
pub fn column_schema_of(
    path: &std::path::Path,
    format: crate::FileFormat,
    as_read: &ReadAs,
) -> Option<Vec<(String, DataType)>> {
    use polars::prelude::{LazyFileListReader, LazyJsonLineReader};
    let lf = match format.descriptor().lines {
        Some(crate::cli::Lines::Delimited(_)) => {
            // Read the way the open will read it. Where the header is and how far the
            // reader looks before settling a type both change what comes back, and a
            // sample that used its own answers would describe a file nobody opened.
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
    // Trimmed, because `trim_csv_column_names` trims what the read produces: one
    // writer's `id, name` and another's `id,name` are the same two columns by the time
    // they are on screen, and a sample that kept the space would call them different
    // and say so in a note about a table that has no such difference.
    let fields: Vec<(String, DataType)> = schema
        .iter()
        .map(|(name, dtype)| (name.trim().to_string(), dtype.clone()))
        .collect();
    Some(fields)
}

/// Whether a schema is what an empty file parses as, rather than a table.
///
/// No columns at all, or exactly one with no name. The second is the one that matters
/// and it is not obvious: a writer that emits a header even on a day with no rows
/// leaves a three-byte file holding `""`, which Polars reads as a single unnamed
/// column — and a schema of one unnamed column is contained in *every* wider schema, so
/// it nests inside anything and says a directory is one table however many tables are
/// really in it.
///
/// Measured on a real share: a month of stock data holding dividends, splits, tickers
/// and daily bars came back as one table because the files the spread landed on were
/// the empty days.
///
/// An unnamed column inside a wider schema is ordinary — it is what a written-out index
/// looks like — so only a schema that is *nothing but* one counts here.
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
    /// Whether a column they share is held in two different types. The half a name test
    /// cannot see: `amount` is an Int64 in one file and a String in the next because
    /// one row said `N/A`, and the read widens it to String for the whole directory
    /// without a word unless this says so.
    pub types_differ: bool,
    /// How many files were actually read, so a caller can say whether a count over them
    /// is exact or a floor.
    pub read: usize,
    /// Whether the files appear to have no header row, so what came back as names is
    /// each file's first row of *data*.
    ///
    /// datui reads a CSV as having a header, so such a directory cannot be read as one
    /// table at all without `--no-header`: every file contributes its own first row as
    /// column names and the union is a wide sheet of nulls. Neither a nesting verdict
    /// nor a note about columns means anything here — this is the thing to say instead.
    pub headerless: bool,
}

impl Sampled {
    /// How the files differ, for the note that says so.
    pub fn disagreement(&self) -> Disagreement {
        // A headerless directory has one thing wrong with it, and the other two would be
        // said about column names that are really data.
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

/// How a directory's files differed, as the read found them.
///
/// Two separate facts because they are two separate things to say, and a directory can be
/// either, both or neither: a column some files lack is the ordinary shape of schema
/// drift, and a column held in two types is what forces the whole directory to the wider
/// one.
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

/// Read a spread of `files` and say what they are.
///
/// The ends and the middle, because names sort and a directory written table by table can
/// start with several files of the same table. Three reads whatever the directory's size:
/// this runs in a listing pass and on the way into an open, and a directory of forty
/// thousand files must cost the same as a directory of four.
pub fn sample_files(
    files: &[std::path::PathBuf],
    format: crate::FileFormat,
    as_read: &ReadAs,
) -> Sampled {
    // Enough files to see a disagreement, and a bound on the reads it takes to find
    // them. A directory written daily has empty days in it — a Saturday's file of nothing
    // — and those carry no columns, so a spread that lands on two of them learns
    // nothing and the directory goes unjudged. Measured on a real share: a month of stock
    // data holding three different tables came back as one, because the first and
    // middle files of that month were a three-byte file and an empty one.
    const WANTED: usize = 3;
    const TRIES: usize = 12;
    let last = files.len().saturating_sub(1);
    // Three anchors — the ends and the middle — because names sort, so a directory
    // written table by table holds each table in a contiguous run and the three land in
    // different runs. Spreading the reads evenly instead is worse, and measurably: the
    // first files that happen to be readable then come from one run, agree with each
    // other, and the directory is called one table.
    //
    // From each anchor, step forward past files with nothing in them. A directory written
    // daily has empty days in it — forty per cent of one real month — and an anchor
    // that lands on one learns nothing, while a neighbour of it is in the same run and
    // answers for that run.
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
            // A file with nothing in it has no columns to disagree about, and is not
            // evidence about the directory either way. Its neighbour is asked instead.
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
    // A file whose "names" are its first row of data. Said rather than guessed around:
    // the directory cannot be read as one table without `--no-header`, so there is no
    // nesting verdict worth reaching and no note about columns worth writing. `nests`
    // stays `Some(false)` so `Enter` steps inside rather than silently building a sheet
    // of nulls, and the columns are dropped rather than offered to the search index as
    // if `4` and `7` were names.
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
    // Not `!nests`: a directory can nest and still be missing a column from one file,
    // which is the ordinary shape of schema drift and exactly what the note is for.
    let widest = names.iter().map(|f| f.len()).max().unwrap_or(0);
    out.columns_differ = names.iter().any(|f| f.len() != widest) || !nests;
    out.types_differ = typed_apart;
    out
}

/// Whether every file's columns are contained in the widest file's.
///
/// The one shape schema evolution produces, and the one that reads cleanly as a union:
/// a file written before a column existed has every column the widest file has, minus
/// the ones added since. Nothing is scored and nothing is thresholded — a file either
/// brings a column no other file has, or it does not.
///
/// This replaced a containment ratio against a 0.5 threshold. The ratio was measured
/// and the threshold sat in a wide gap, but every counter-example found was a directory
/// landing on the wrong side of a number: two tables joined on one key scored exactly
/// 0.5, and a dataset grown from ten columns to fifty with one dropped along the way
/// scored 0.196 — *below* the 0.200 of unrelated tables sharing a key. A dataset that
/// drifted could not be told from tables that never agreed, by any statistic over
/// column overlap, because the two produce the same overlaps. So the question changed
/// instead of the number: not "how much do these agree" but "does any file bring
/// something the others cannot account for".
///
/// A directory that fails this is not refused. It is one keystroke further away — the row
/// goes inside instead of opening, and the `(all files)` row inside it opens the union
/// anyway. That is what makes a strict rule affordable here.
///
/// Fewer than two files is one table by definition, and so is a directory whose files all
/// have no columns to disagree about.
pub fn is_nested(files: &[Vec<String>]) -> bool {
    let Some(widest) = files.iter().max_by_key(|f| f.len()) else {
        return true;
    };
    let widest: std::collections::BTreeSet<&str> = widest.iter().map(String::as_str).collect();
    files
        .iter()
        .all(|file| file.iter().all(|name| widest.contains(name.as_str())))
}

/// The top-level column names in a list of Parquet leaf paths.
///
/// A footer names every leaf, so a struct or a list arrives as `inputs.list.element.
/// address` and its wrappers are an encoding choice: the same column written by
/// parquet-mr and by Arrow gives different leaves. Comparing those would make a
/// dataset whose writer changed look like two tables, so the comparison is over the
/// columns a reader sees. Order is kept and duplicates dropped, since many leaves
/// share one root.
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
    /// Per file, in the order given, the columns not read from it and the type that
    /// file holds each of them in. Empty for a file whose types all fit.
    ///
    /// The type is kept because it is the only way back to the values: reading such a
    /// column as text means reading it from each file at the type that file wrote,
    /// which the footers know and nothing else does.
    pub omitted: Vec<Vec<(PlSmallStr, DataType)>>,
    /// Files whose footer could not be read, by index into the files given.
    pub unreadable: Vec<usize>,
    /// Files whose footer was read, readable or not.
    pub files: usize,
    /// The distinct ways this dataset's files differ from its schema. Group 0 is
    /// always "nothing missing", which is what a file not read counts as.
    pub groups: Vec<DriftGroup>,
    /// Per file, in the order given, its group in `groups`.
    pub file_group: Vec<u32>,
    pub origin: SchemaOrigin,
    /// Columns being read as text from every file rather than as the type most rows
    /// have. Empty for a dataset as its footers found it.
    pub read_as_text: Vec<PlSmallStr>,
    /// Files whose footer said they hold no rows, among those read.
    pub empty_files: usize,
    /// The middle row group's compressed size, over every row group of every footer
    /// read. `None` when no footer reported one.
    pub median_row_group_bytes: Option<usize>,
    /// The middle file's size, over the footers read. `None` when none was read.
    pub median_file_bytes: Option<usize>,
    /// Where each column that is not in every file sits, for those whose files fall
    /// into a shape worth naming. Empty for a dataset with no partitions, and for one
    /// whose footers were sampled — a file whose footer was not read looks like a file
    /// missing nothing, and a range drawn over those would be a guess.
    pub column_ranges: HashMap<PlSmallStr, ColumnRange>,
    /// The distinct ways the dataset's files are partitioned, and how many files are
    /// laid out each way, commonest first. One entry, or none, for a dataset whose
    /// directories agree — which is nearly all of them.
    ///
    /// From the names of every file, not from the footers: this is the one thing the
    /// listing knows that reading a file cannot tell you.
    pub partition_layouts: Vec<(Vec<String>, usize)>,
    /// The layouts past the ones kept, and the files under them. A dataset with a key
    /// per file is not worth remembering in full, but a note that counts what it does
    /// not name has to count all of it.
    pub partition_layouts_dropped: (usize, usize),
    /// What the listing passed over on the way to the files it read. From the listing,
    /// which had to look at every name anyway.
    pub skipped: SkippedFiles,
    /// How many file names were read to find the layouts, including the ones with no
    /// partition keys at all.
    pub listed_files: usize,
}

/// What a file is missing relative to the dataset's schema. Files that are missing the
/// same things share a group, so a row need only carry its group to know how to draw.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct DriftGroup {
    /// Columns the file does not have. Their cells are absent, not null.
    pub absent: Vec<PlSmallStr>,
    /// Columns the file has in a type the dataset's column cannot hold, so they are
    /// not read from it. Their cells are a conflict, not null.
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

    /// The dataset with the ways its files are partitioned counted from their names.
    ///
    /// `root` is the dataset as opened; the keys are taken from below it. A directory
    /// above the root is not in dispute — opening `run=7/` for a dataset partitioned
    /// by date does not make `run` one of the things its directories disagree about, and
    /// counting it made every file look like it disagreed with every other.
    ///
    /// Keys are compared as a *set*: `y=1/m=1` and `m=2/y=2` partition by the same two
    /// things and Polars matches hive columns by name, not position, so a dataset that
    /// mixes the orders reads perfectly well and has nothing to disagree about.
    pub fn with_partition_layouts(mut self, root: &str, paths: &[String]) -> DatasetSchema {
        /// Layouts kept. The note names two and counts the rest, and a dataset with a
        /// key per file would otherwise hold half a million of them for the life of
        /// the frame to say "and 499,998 others".
        const KEPT: usize = 64;
        // Keyed by the spelling rather than scanned for: a dataset whose every file
        // has its own layout is pathological, but it should be slow to read, not
        // quadratic. At half a million files the scan took three minutes.
        let mut counts: HashMap<Vec<String>, usize> = HashMap::new();
        for path in paths {
            // A path the root is not a prefix of is one this cannot place. Skipping it
            // is the only safe answer: scanning the whole path instead would count the
            // keys above the dataset, which both invents disagreements between files
            // that agree and hides the real ones between files that do not.
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
        // Commonest first, and by the keys themselves where two are equally common:
        // a HashMap hands them back in no order at all, so without the tie-break the
        // same dataset would name a different layout from one open to the next.
        counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        // Counted before they are dropped, or the note's "and N other ways" would be
        // the ways it happens to still be holding rather than the ways there are.
        let dropped = &counts[counts.len().min(KEPT)..];
        self.partition_layouts_dropped =
            (dropped.len(), dropped.iter().map(|(_, files)| files).sum());
        counts.truncate(KEPT);
        self.partition_layouts = counts;
        self.listed_files = paths.len();
        self.column_ranges = self.ranges_of_columns(root, paths);
        self
    }

    /// Where each column that is not in every file sits, by partition. See
    /// [`ColumnRange`].
    ///
    /// Nothing for a sampled dataset: `file_group` says a file whose footer was not
    /// read is missing nothing, which is the right answer for drawing cells and the
    /// wrong one for saying where a column begins.
    fn ranges_of_columns(&self, root: &str, paths: &[String]) -> HashMap<PlSmallStr, ColumnRange> {
        // A file whose footer was not read is recorded as missing nothing, which is the
        // right answer for drawing its cells and the wrong one for saying where a
        // column begins. That is true of a sampled dataset and equally true of a file
        // whose footer would not parse, which is a file datui never opened either.
        if matches!(self.origin, SchemaOrigin::FooterSample { .. })
            || !self.unreadable.is_empty()
            || self.file_group.len() != paths.len()
        {
            return HashMap::new();
        }
        // Per column: the first file that has it, the last that does not, and the
        // partitions of the files that do — two of them is already enough to know it is
        // not "only" one, so the third is never kept.
        struct Seen {
            first_present: Option<usize>,
            last_absent: Option<usize>,
            /// The partitions of the files that have it, and of the files that do not.
            /// Two of the first is enough — more than one and the column is not "only"
            /// anywhere. Two of the second is a sample: the test is whether any file
            /// lacking it is somewhere the claim does not cover, and a sample can miss
            /// one, which costs a note rather than makes a wrong one.
            with: Vec<String>,
            without: Vec<String>,
            /// A file that has it and sits under no partition at all — one at the root
            /// beside the partition directories. There is no "all under" to be had then:
            /// the column is somewhere this cannot name.
            unplaced: bool,
        }
        let partition_of = |index: usize| -> Option<String> {
            let below = paths.get(index)?.strip_prefix(root)?;
            let values = partition_values_of(below);
            (!values.is_empty()).then(|| values.join("/"))
        };
        // Whether "before" means the same thing to the listing and to a reader. The
        // listing is sorted bytewise, which puts `part=10` before `part=2`; where the
        // two disagree there is no honest way to say a column starts somewhere, so
        // nothing does.
        let reads_in_order = (0..paths.len())
            .filter_map(&partition_of)
            .collect::<Vec<_>>()
            .windows(2)
            .all(|pair| natural_cmp(&pair[0], &pair[1]) != std::cmp::Ordering::Greater);
        // The columns worth asking about, and for each group the ones it lacks — both
        // settled once rather than per file. There are few groups and many files, and
        // a scan of every column's name against every group's absent list, per file,
        // is the shape of thing this file has been caught by before.
        let drifting: Vec<&ColumnDrift> = self
            .columns
            .iter()
            .filter(|column| column.present_in > 0 && column.present_in < self.files)
            .collect();
        if drifting.is_empty() {
            return HashMap::new();
        }
        // Built by walking each group's own absent list, not by asking every column
        // whether it is in it: a dataset can have a group per file, and asking is a
        // scan of the list per column per group.
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
        // By index rather than by name: `drifting` already carries a stable position for
        // every column, and a hash of the name per file per column is most of what this
        // costs — measured at 134ms over twenty thousand files, against fifteen.
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
                // One partition holds every file that has it — and at least one file
                // that does not is somewhere else, or "only" says nothing while
                // sounding as though it does.
                if !entry.unplaced
                    && entry.with.len() == 1
                    // Somewhere else, as a place rather than as a different string:
                    // a file at `y=2024/m=03` is a file under `y=2024`.
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
                // Every file without it comes before every file with it — and the
                // reader will check that against the partition values, not against the
                // order the listing happened to be in.
                let last_absent = entry.last_absent?;
                // `unplaced` is not asked here, unlike above, and the difference is in
                // what the two sentences claim. "Only X" is about where every file with
                // the column is, so one that is nowhere nameable makes it false. "None
                // before X" is about order: a file with no partition sorts where the
                // listing puts it, and one after the boundary does not contradict a
                // word of it. Refusing here would cost a true note to buy nothing.
                if last_absent > first || !reads_in_order {
                    return None;
                }
                let (ends, begins) = (partition_of(last_absent)?, partition_of(first)?);
                // And the boundary is a boundary: a partition half of whose files have
                // the column is not one the column begins at.
                // And the boundary is a boundary: not the same place under another
                // spelling, and not one directory inside the other.
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

    /// This dataset as it reads with `as_text` read as text from every file.
    ///
    /// What the panel shows and what the table draws from, not what the scan is built
    /// from — the scan needs the types the footers found, which is why `omitted` is
    /// carried through untouched and why the caller keeps the original alongside.
    ///
    /// Those columns stop conflicting: their cells hold a value from every file, so
    /// nothing is unread, nothing is left out of a filter or sort, and the note that
    /// said the column was not read there has nothing left to say. `absent` is left
    /// alone — a file that never had the column still has none to show.
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
                // `widened` is left alone. A file whose type merely widens into the
                // column's is still read at the column's type — an integer in a float
                // column still reads as `7.0` — so the note saying a type gave way is
                // still true, and removing it would leave the `7.0` unexplained.
            }
        }
        for group in &mut out.groups {
            group.unread.retain(|name| !as_text.contains(name));
        }
        out.read_as_text = as_text.to_vec();
        out
    }

    /// Whether any file is missing anything. When nothing is, the scan is one plain
    /// read and rows need carry nothing.
    pub fn drifts(&self) -> bool {
        self.groups.iter().any(|g| !g.is_empty())
    }
}

/// Footers read at once: one wave. A footer read is waiting, not computing — on a
/// network mount or a store it is a few round trips — so this is not the core count.
/// A dataset of more files than this opens from its ends and reads the rest behind.
pub const FOOTERS_AT_ONCE: usize = 64;

/// Footers in the form the shape cache keeps them: the schemas gathered into a table
/// and referred to by index, since a dataset of ten thousand files usually has one
/// schema, and writing each file's columns out in full would make the cache larger
/// than the footers it saves reading.
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

/// The footers a cache kept, as a fresh pass would have read them. `file_bytes` is
/// each file's size from the listing, which the cache does not hold.
///
/// `None` for a file whose footer would not read, which is how the pass reports one
/// and so how the cache has to give it back. The whole entry is refused when it
/// disagrees with itself or with the listing.
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

/// Run `hook` before each local footer read under `dir` until the guard drops. For
/// tests: to count a pass's reads, or hold one to stand in for a slow filesystem.
/// Keyed by directory so tests running side by side do not see each other's reads.
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

/// Footers read before a dataset opens. Past this many files the reads cost more than
/// the schema is worth, so a spread sample stands in for the rest. Documented in
/// `docs/user-guide/large-datasets.md`; a fixed threshold, not a setting.
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

/// The first file and the last, which is what a dataset opens from while the rest of
/// its footers are still being read. Ascending, and one index when there is one file.
///
/// The last by name, not by date: the files are sorted by key, and a dataset whose
/// partition values are not zero-padded puts `month=9` after `month=10`. The pair is a
/// heuristic either way — between the oldest shape and a recent one lies most of what a
/// dataset disagrees about — and everything it misses arrives with the rest.
pub fn ends_of(files: usize) -> Vec<usize> {
    match files {
        0 => Vec::new(),
        1 => vec![0],
        n => vec![0, n - 1],
    }
}

/// The schema of a dataset of `files` files whose footers at the indices `read` were
/// fetched, in that order.
///
/// The union is over the footers read; the scan is over every file, so the per-file
/// findings are spread back across the full list. A file not read omits nothing.
///
/// [`DatasetSchema::files`] stays the number of footers *read*, not `files`, and every
/// count taken against it — the notes' denominator, how many files hold no rows — is
/// therefore over the sample. That is the honest population: datui knows nothing about
/// a file it did not open. Only `omitted`, `file_group` and `unreadable`, which the
/// scan indexes by file, are spread to the full length.
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

/// The paths whose footers were read, out of the paths given.
///
/// A footer datui could not read is a file Polars cannot read either, and left in the
/// scan it does not merely go unread: the first page takes the whole dataset down with
/// it, so a directory with one file mid-write opens on an error rather than on the rows
/// of its other files. The dataset still counts them — that is what the note is for — but
/// the scan is over the ones that will open.
pub fn readable_paths<'a>(paths: &'a [String], unreadable: &[usize]) -> Cow<'a, [String]> {
    if unreadable.is_empty() {
        // Which is nearly always, and a dataset can be millions of paths.
        return Cow::Borrowed(paths);
    }
    // `unreadable` is ascending — `union_file_schemas` collects it in order and
    // `union_sampled` remaps it through an ascending sample — so this is a search
    // rather than a scan of it per path.
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

/// Fold every file's footer into one schema. `files` is in scan order, so the last
/// readable entry is the newest file; `None` is a file whose footer could not be read.
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

    // What each file is missing, and which files are missing the same things. Group 0
    // is "nothing missing", so a file that was never read falls into it and draws as
    // an ordinary file would.
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

/// Whether one partition path holds another: `y=2024` holds `y=2024/m=03`, and a file
/// in the second is a file in the first.
///
/// Compared as places rather than as strings. `only y=2024` is a claim about a directory
/// tree, so a file at `y=2024/m=03` without the column is a file under `y=2024`
/// without it, and the claim is false — even though the two strings differ.
fn partition_holds(outer: &str, inner: &str) -> bool {
    inner == outer
        || inner
            .strip_prefix(outer)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// Whether two partition paths name the same place, however they are written.
///
/// `m=03` is March and so is `m=3`: a backfill that wrote one beside a job that wrote the
/// other leaves two directories for one month. And hive columns are matched by name, so
/// `y=2024/m=03` and `m=03/y=2024` are one partition written in two orders. Neither pair
/// is equal as a string, and a claim about either is a claim about both.
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

/// Compares partition values the way a reader does: `part=2` before `part=10`.
///
/// The listing is sorted bytewise, which puts `part=10` before `part=2`. A note saying
/// "from `X` on" is read against the values, so where the two orders disagree the
/// sentence is false — see [`ends_of`], which has to live with the same thing.
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
                // Leading zeros do not make a number bigger: `m=03` and `m=3` are one
                // month, and this says so. Two directories spelling it both ways are two
                // directories for one place, which `same_place` is about.
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

/// The `key=value` segments of a path below the dataset's root, in the order they are
/// written. The values as well as the keys, which is what tells one partition from
/// another rather than one layout from another.
///
/// Not sorted and not deduplicated, unlike the keys: an order is a set of columns, a
/// partition is a place, and a place is where the path says it is. One consequence is
/// that `y=2024/m=03` and `m=03/y=2024` are written differently while naming one
/// partition — hive matches its columns by name, not by position. Nothing else notices:
/// the layouts note compares which keys a directory uses, not the order, so those two
/// agree. `same_place` is what keeps a note off a place that is written down twice.
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

/// The hive partition keys in a path, in order: `a=1/b=2/f.parquet` is `[a, b]`.
///
/// A segment is a partition only if it has a key before the `=`. The file's own name
/// is never one — `data/x=1/2024=05.parquet` partitions by `x`, not by `x` and `2024`.
fn partition_keys_of(path: &str) -> Vec<String> {
    let mut keys: Vec<String> = Vec::new();
    // A backslash separates on Windows and is an ordinary character in a Linux file
    // name, where splitting on it would both break a legitimate name and invent a
    // layout difference out of one directory.
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
    // A set, not a sequence: hive columns are matched by name, so `y=1/m=1` and
    // `m=2/y=2` are the same two partitions written in two orders and nothing about
    // them is in dispute. Sorted so the two spell the same, and deduplicated so a tree
    // that repeats a key is one thing rather than two.
    keys.sort();
    keys.dedup();
    keys
}

/// The middle value of `sizes`, or the lower of the middle two. `None` when empty.
///
/// The middle rather than the mean: one file written by a different job, or one
/// backfill that rewrote a year into a single row group, drags a mean somewhere no
/// actual row group is, and the note would then describe a dataset that does not exist.
fn median(sizes: impl Iterator<Item = usize>) -> Option<usize> {
    let mut sizes: Vec<usize> = sizes.collect();
    if sizes.is_empty() {
        return None;
    }
    sizes.sort_unstable();
    Some(sizes[(sizes.len() - 1) / 2])
}

/// The type to read a column as, given every `(type, rows)` a file reported for it.
///
/// The widest lossless type when they all fit together; otherwise the type behind the
/// most rows, preferring one that also covers other files. Ties go to the type seen
/// first, which is the newest file's.
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
    // A type no file has can still be the answer: Int32 and Float32 files read as
    // Float64. Offer the fold of everything each type widens with as a candidate too.
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

/// Whether a file storing `from` can be read into a column of `to` without loss. Mirrors
/// the cast policy [`crate::cloud_hive::lenient_scan`] gives Polars; a type that does not
/// fit would fail the scan, so the column is left unread in that file instead.
pub fn fits(from: &DataType, to: &DataType) -> bool {
    widen(from, to).as_ref() == Some(to)
}

/// The narrowest type both `a` and `b` read into without loss, or `None` when there is
/// none.
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

/// `Int32` and `Int64` widen to `Int64`; a signed and an unsigned type need one wide
/// enough for both, which `UInt64` never is.
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

/// A struct has every field either side has; shared fields widen. Field order is `a`'s,
/// then `b`'s additions, so the newest file's layout leads.
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

/// Partition column names from one file's path, in path order: every `key=value`
/// segment, each key once.
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

/// A listed dataset's partition columns and the values that type them, from its first
/// and newest files' keys, `/`-separated. The newest names the columns, since a key
/// added later is in it; both give values. Local directories and cloud prefixes derive
/// them here alike, so a tree is the same table from either.
pub fn partitions_of_listing(first: &str, newest: &str) -> (Vec<String>, Vec<(String, String)>) {
    let values = [first, newest]
        .iter()
        .flat_map(|key| key.split('/'))
        .filter_map(|segment| segment.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    (partition_columns_of_key(newest), values)
}

/// A dataset's row count from its footers, each read once.
///
/// It starts from the footers the open already read — the two ends, or a sample — and
/// reads only the rest. Once every file's footer is in, the whole set is handed back
/// once, for the shape cache, so a reopen reads none.
pub struct FooterCount<F> {
    files: usize,
    /// The files the count answers for, as indices, in order: those whose footer the
    /// open could read.
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
    /// A count of `counted` among `files` files, starting from the footers `known`
    /// already holds, given as each one's index.
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

    /// Count, reading the footers not yet in with `read`, which answers in the order it
    /// is asked, or `None` when the reads were abandoned. One that would not read before
    /// is tried again: a read can fail for a moment's trouble as well as a broken file.
    /// Holds the footers while it reads, so two counts at once do not both read the
    /// same ones.
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
                // Counted, but a file the open could not read stays unread, so there
                // is nothing whole to keep and nothing more to hold on to.
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

/// The column the scan writes each row's position in the dataset into, so a cell can be
/// traced back to the file it came from and told whether that file had the column at
/// all. Never shown, filtered, sorted or exported: it is in the buffer the display is
/// sliced from, and the display only ever projects the column order.
pub const DRIFT_COLUMN: &str = "__datui_row";

/// A file that is missing nothing, for a group id with no entry of its own.
static NOTHING_MISSING: DriftGroup = DriftGroup {
    absent: Vec::new(),
    unread: Vec::new(),
};

/// How a dataset's files differ, in the form the scan needs: what each file is missing,
/// and where its rows begin in the dataset, by the path or URL the scan names it by.
#[derive(Debug, Clone, Default)]
pub struct ScanDrift {
    group_of: HashMap<String, u32>,
    /// Where each file's rows start in the dataset. The scan numbers a run's rows from
    /// its first file's entry, so the numbering survives reading only a window of files.
    row_of: HashMap<String, usize>,
    /// Per file, the type it holds each of its conflicting columns in. Only files that
    /// conflict have an entry, which is the few.
    stored_of: HashMap<String, Vec<(PlSmallStr, DataType)>>,
    pub groups: Vec<DriftGroup>,
}

impl ScanDrift {
    /// `None` when every file agrees with the schema, or when the rows of each file are
    /// not known — either way the scan is one plain read and rows carry nothing extra.
    ///
    /// `file_rows` is each file's row count, in the order of `paths`.
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

    /// The type the file the scan names `path` holds `column` in, when that is not the
    /// type the column is read as. `None` when the file agrees, or has no such column.
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

/// A scan of `paths` into `schema` that reads files written at different times:
/// columns and nested fields a file lacks are filled with nulls, ones it has beyond the
/// schema are ignored, and integers, floats and datetime units widen. Polars'
/// `scan_parquet` offers only the first of those, and a Bitcoin transactions file from
/// 2015 fails against the 2026 schema without the rest.
///
/// When `drift` is given, every row carries its position in the dataset in
/// [`DRIFT_COLUMN`], which is what lets a cell be traced to its file and a null told
/// from a column that file never had.
///
/// The scan splits only where it has to: a column a file stores in another type must be
/// left out of *that* file's read, so consecutive files omitting the same columns are
/// one scan and the scans are concatenated in file order. A file merely missing a
/// column needs no split — `MissingColumnsPolicy::Insert` already reads it as null —
/// so the common case stays a single scan however many files disagree.
///
/// `as_text` names columns to read as text from every file instead of leaving them out
/// of the ones that disagree. Such a column is read at the type each file *disagrees*
/// in and cast to text after, which is the only way to see the values a conflict hides:
/// Polars' scan can widen an integer and change a datetime's unit, but it has no policy
/// for reading a number as a string, and no way to hand back a column it was not told
/// the type of. So the split is finer here — a run is a stretch of files that agree on
/// the type of every `as_text` column as well as on what they are missing.
///
/// A file whose type merely *widens* into the column's is read at the column's type,
/// not its own, because only conflicting types are recorded: an integer in a column
/// read as a float reads as `7.0`. Nothing is hidden by that — a widened value was
/// always on screen — but it is not the file's own spelling. The same goes for a file
/// whose footer could not be read, and for one outside the sample on a dataset too
/// large to read every footer: datui does not know what those hold, so it asks for the
/// column's type and they are no better off than before.
///
/// A column [`can_read_as_text`] refuses, or that the schema does not have, is dropped
/// from `as_text` and read as it was.
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
    // A column is read as text only where the schema has it and every type it is
    // stored in can be shown as text. A caller that asks for more than that gets the
    // column as it was rather than a scan that fails: the cast refusing would take
    // the whole read with it, including the files that never disagreed.
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
    // What a run must agree on: what it leaves out, and the type of everything read as
    // text, since that is what its own read schema is built from.
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

/// Whether a column of this type can be shown as text.
///
/// Reading a conflicting column as text is a cast, and Polars cannot cast every type
/// to a string: a duration and a list refuse outright, and binary refuses the moment
/// its bytes are not UTF-8 — which is most of why a column is binary. A cast that
/// refuses fails the whole scan, including the files that never disagreed, so this is
/// asked before the offer is made rather than after it is taken.
///
/// Answered by type and not by value, so binary is refused whatever it holds: a
/// column that renders for one page and fails on the next is worse than one that was
/// never offered. `types_the_cast_agrees_with_are_exactly_the_ones_offered` keeps this
/// honest against Polars itself.
pub fn can_read_as_text(dtype: &DataType) -> bool {
    match dtype {
        // Not text at all, and not convertible: the cast errors rather than escaping.
        DataType::Binary | DataType::BinaryOffset => false,
        DataType::Duration(_) => false,
        // Nested sequences have no string form in Polars 0.55.
        DataType::List(_) | DataType::Array(_, _) => false,
        // A struct prints as `{1,"a"}`, writing its fields itself rather than casting
        // them, so it manages inner types a column of that type could not.
        DataType::Struct(_) => true,
        DataType::Unknown(_) => false,
        _ => true,
    }
}

impl ColumnDrift {
    /// Whether this column can be read as text: every type any file holds it in has to
    /// be one that can be shown as text, the one it is read as included. One file's
    /// list column is enough to rule it out, because that file's cast is the one that
    /// would fail.
    pub fn can_read_as_text(&self) -> bool {
        can_read_as_text(&self.dtype) && self.conflicting_types.iter().all(can_read_as_text)
    }
}

/// `schema` with every column in `as_text` spelled as text.
///
/// For a caller to say what it now holds — [`lenient_scan`] does not need it, since a
/// run is read at its own files' types and the cast decides the result's. The columns
/// keep their places: a column that moved when it was read differently would be a
/// second change the user did not ask for.
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

/// One run of files that are missing the same things. `stamp` is the group to write
/// into [`DRIFT_COLUMN`], and its presence also means the run selects the dataset's
/// column order so the runs concatenate.
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
            // Read at the type this run's files wrote, not the one it will be shown
            // as: the reader has to be told what is actually in the file.
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
        // Numbering the rows from where this run begins costs one column and no extra
        // read, and it is what survives a sort: a row keeps its place in the dataset
        // however the view is reordered.
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
        // Now the values are in hand, they become text. Every run casts, including one
        // whose files already agree: they are all being shown as text, and a run that
        // skipped the cast would not concatenate with the rest. A date past the
        // calendar, on which Polars' cast panics, becomes its stored number.
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
        // Runs concatenate only if they agree on column order, and the row index
        // arrives first, so put it back at the end where the state expects it.
        let mut ordered: Vec<Expr> = schema.iter_names().map(|name| col(name.clone())).collect();
        ordered.push(col(DRIFT_COLUMN));
        lf = lf.select(ordered);
    }
    Ok(lf)
}

#[cfg(test)]
mod tests;
