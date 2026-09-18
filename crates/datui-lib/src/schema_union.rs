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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
/// Opening a folder of many files reads a footer from each before a row is shown, and
/// on a few thousand files that is seconds of a screen that says only "Caching schema".
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
}

impl FooterProgress {
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
}

/// What one file's footer said, short of the data.
#[derive(Debug, Clone)]
pub struct FileSchema {
    pub schema: Arc<Schema>,
    pub rows: usize,
    /// The file's size on disk or in the store.
    pub file_bytes: usize,
    /// Compressed bytes of each row group, in file order: what crosses the wire for
    /// that group, not what it occupies once decoded.
    ///
    /// A row group is the unit a reader fetches: a page of rows anywhere inside one
    /// costs the whole of it. How big they are is therefore what a remote dataset
    /// costs to scroll, and it is in the footer datui already reads.
    pub row_group_bytes: Vec<usize>,
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
    /// The distinct ways the dataset's files are partitioned, and how many files are
    /// laid out each way, commonest first. One entry, or none, for a dataset whose
    /// folders agree — which is nearly all of them.
    ///
    /// From the names of every file, not from the footers: this is the one thing the
    /// listing knows that reading a file cannot tell you.
    pub partition_layouts: Vec<(Vec<String>, usize)>,
    /// The layouts past the ones kept, and the files under them. A dataset with a key
    /// per file is not worth remembering in full, but a note that counts what it does
    /// not name has to count all of it.
    pub partition_layouts_dropped: (usize, usize),
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
    /// `root` is the dataset as opened; the keys are taken from below it. A folder
    /// above the root is not in dispute — opening `run=7/` for a dataset partitioned
    /// by date does not make `run` one of the things its folders disagree about, and
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

/// Footers read before a dataset opens. Past this many files the reads cost more than
/// the schema is worth, so a spread sample stands in for the rest. Documented in
/// `docs/user-guide/loading-data.md`; a fixed threshold, not a setting.
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
    footers: &[Option<FileSchema>],
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

/// Fold every file's footer into one schema. `files` is in scan order, so the last
/// readable entry is the newest file; `None` is a file whose footer could not be read.
pub fn union_file_schemas(files: &[Option<FileSchema>], origin: SchemaOrigin) -> DatasetSchema {
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
            sightings[index].push((dtype.clone(), file.rows));
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
        empty_files: files.iter().flatten().filter(|f| f.rows == 0).count(),
        median_file_bytes: median(files.iter().flatten().map(|f| f.file_bytes)),
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
            crate::widgets::datatable::partition_dtype(name, file_schema, values),
        );
    }
    for (name, dtype) in file_schema.iter() {
        if !part_set.contains(name.as_str()) {
            merged.with_column(name.clone(), dtype.clone());
        }
    }
    merged
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
        // skipped the cast would not concatenate with the rest.
        let texts: Vec<Expr> = read_as
            .iter()
            .map(|(name, _)| col(name.clone()).cast(DataType::String).alias(name.clone()))
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
mod tests {
    use super::*;

    fn file(columns: &[(&str, DataType)], rows: usize) -> Option<FileSchema> {
        let mut schema = Schema::with_capacity(columns.len());
        for (name, dtype) in columns {
            schema.with_column((*name).into(), dtype.clone());
        }
        Some(FileSchema {
            schema: Arc::new(schema),
            rows,
            file_bytes: 0,
            row_group_bytes: Vec::new(),
        })
    }

    fn union(files: &[Option<FileSchema>]) -> DatasetSchema {
        union_file_schemas(files, SchemaOrigin::AllFooters(files.len()))
    }

    /// The names alone of the columns not read from file `index`.
    fn omitted_names(union: &DatasetSchema, index: usize) -> Vec<String> {
        union.omitted[index]
            .iter()
            .map(|(name, _)| name.to_string())
            .collect()
    }

    fn names(schema: &Schema) -> Vec<String> {
        schema.iter_names().map(|n| n.to_string()).collect()
    }

    /// Reading a conflicting column as text shows the values the conflict was hiding.
    ///
    /// Three files, all disagreeing on `n`: an integer, text, and a float. Whichever
    /// type wins, the other two files' values are unreachable — the column is not read
    /// from them at all, and their cells are `≠`. Read as text, every value is there,
    /// in dataset order, spelled the way its own file stored it.
    #[test]
    fn a_conflicting_column_read_as_text_shows_every_file_s_values() {
        use polars::prelude::{ParquetWriter, df};

        let dir = tempfile::tempdir().unwrap();
        let mut paths = Vec::new();
        let mut write = |name: &str, mut frame: polars::prelude::DataFrame| {
            let path = dir.path().join(name);
            let file = std::fs::File::create(&path).unwrap();
            ParquetWriter::new(file).finish(&mut frame).unwrap();
            paths.push(path.to_string_lossy().to_string());
        };
        // The integer file has the most rows, so `n` is read as an integer.
        write(
            "a.parquet",
            df!("id" => &[0i64, 1, 2], "n" => &[10i64, 20, 30]).unwrap(),
        );
        write("b.parquet", df!("id" => &[3i64], "n" => &["x"]).unwrap());
        // Boolean, not a float: a float would widen with the integer rather than
        // conflict with it, and then there would be only one conflict to show.
        write("c.parquet", df!("id" => &[4i64], "n" => &[true]).unwrap());

        let footers: Vec<Option<FileSchema>> = vec![
            file(&[("id", DataType::Int64), ("n", DataType::Int64)], 3),
            file(&[("id", DataType::Int64), ("n", DataType::String)], 1),
            file(&[("id", DataType::Int64), ("n", DataType::Boolean)], 1),
        ];
        let dataset = union_file_schemas(&footers, SchemaOrigin::AllFooters(3));
        assert_eq!(
            dataset.schema.get("n"),
            Some(&DataType::Int64),
            "the integer file has the most rows"
        );
        let drift = ScanDrift::new(&paths, &dataset, &[3, 1, 1]).expect("the files disagree");

        // As the dataset opens: the other two files' values are not read at all.
        let plain = lenient_scan(&paths, dataset.schema.clone(), None, Some(&drift), &[])
            .unwrap()
            .collect()
            .unwrap();
        let n = plain.column("n").unwrap();
        assert_eq!(
            (0..n.len())
                .map(|i| n.get(i).unwrap().to_string())
                .collect::<Vec<_>>(),
            ["10", "20", "30", "null", "null"],
            "the text and boolean files hold a value, and it is not one this column \
             can carry"
        );

        // Read as text: every file's value, spelled as that file stored it.
        let as_text = [PlSmallStr::from("n")];
        let text = lenient_scan(&paths, dataset.schema.clone(), None, Some(&drift), &as_text)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(
            text.column("n").unwrap().dtype(),
            &DataType::String,
            "the column is text now"
        );
        let n = text.column("n").unwrap().str().unwrap();
        assert_eq!(
            n.iter().collect::<Vec<_>>(),
            [Some("10"), Some("20"), Some("30"), Some("x"), Some("true")],
            "and holds what each file wrote, spelled as that file's own type prints"
        );
        let ids = text.column("id").unwrap().i64().unwrap();
        assert_eq!(
            ids.into_no_null_iter().collect::<Vec<_>>(),
            [0, 1, 2, 3, 4],
            "in dataset order, with the rows still lined up against their ids"
        );
        assert_eq!(
            text.column(DRIFT_COLUMN)
                .unwrap()
                .u32()
                .unwrap()
                .into_no_null_iter()
                .collect::<Vec<_>>(),
            [0, 1, 2, 3, 4],
            "and each row still knows its place in the dataset"
        );
    }

    /// A column a file simply does not have stays null when the column is read as text,
    /// rather than becoming the word "null" or borrowing a neighbour's value.
    #[test]
    fn reading_as_text_leaves_a_file_without_the_column_alone() {
        use polars::prelude::{ParquetWriter, df};

        let dir = tempfile::tempdir().unwrap();
        let mut paths = Vec::new();
        let mut write = |name: &str, mut frame: polars::prelude::DataFrame| {
            let path = dir.path().join(name);
            let f = std::fs::File::create(&path).unwrap();
            ParquetWriter::new(f).finish(&mut frame).unwrap();
            paths.push(path.to_string_lossy().to_string());
        };
        write(
            "a.parquet",
            df!("id" => &[0i64, 1], "n" => &[10i64, 20]).unwrap(),
        );
        // No `n` at all.
        write("b.parquet", df!("id" => &[2i64]).unwrap());
        write("c.parquet", df!("id" => &[3i64], "n" => &["x"]).unwrap());

        let footers: Vec<Option<FileSchema>> = vec![
            file(&[("id", DataType::Int64), ("n", DataType::Int64)], 2),
            file(&[("id", DataType::Int64)], 1),
            file(&[("id", DataType::Int64), ("n", DataType::String)], 1),
        ];
        let dataset = union_file_schemas(&footers, SchemaOrigin::AllFooters(3));
        let drift = ScanDrift::new(&paths, &dataset, &[2, 1, 1]).expect("the files disagree");
        let as_text = [PlSmallStr::from("n")];
        let text = lenient_scan(&paths, dataset.schema.clone(), None, Some(&drift), &as_text)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(
            text.column("n")
                .unwrap()
                .str()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some("10"), Some("20"), None, Some("x")],
            "the file with no `n` has none to show"
        );
    }

    /// The types the predicate offers are exactly the types Polars will cast.
    ///
    /// Asked of Polars rather than remembered: the list of what casts to a string is
    /// Polars' to change, and a predicate that drifts from it either hides a column
    /// that would have read fine or offers one whose cast fails the whole scan. Each
    /// case carries a real value, because an all-null column casts from anything.
    #[test]
    fn types_the_cast_agrees_with_are_exactly_the_ones_offered() {
        use polars::prelude::*;

        let mk = |dtype: DataType| -> Column {
            Series::new("x".into(), [1i64, 2])
                .cast(&dtype)
                .unwrap_or_else(|e| panic!("cannot build a {dtype:?} column: {e}"))
                .into()
        };
        let mut cases: Vec<(DataType, Column)> = vec![
            DataType::Int64,
            DataType::Float64,
            DataType::Boolean,
            DataType::Date,
            DataType::Time,
            DataType::Datetime(TimeUnit::Microseconds, None),
            DataType::Duration(TimeUnit::Milliseconds),
            DataType::Decimal(10, 2),
            DataType::List(Box::new(DataType::Int64)),
        ]
        .into_iter()
        .map(|dtype| (dtype.clone(), mk(dtype)))
        .collect();
        cases.push((DataType::String, Series::new("x".into(), ["a", "b"]).into()));
        // Bytes that are not text, which is most of why a column is binary.
        cases.push((
            DataType::Binary,
            Series::new("x".into(), [&[0xffu8, 0xfe][..], &[0x41][..]]).into(),
        ));
        let plain =
            StructChunked::from_series("x".into(), 2, [Series::new("a".into(), [1i64, 2])].iter())
                .unwrap()
                .into_series();
        cases.push((plain.dtype().clone(), plain.into()));
        // A struct prints its fields itself rather than casting them, so it manages
        // inner types that a column of that type could not.
        let inners: [Series; 3] = [
            Series::new("a".into(), [1i64, 2])
                .cast(&DataType::Duration(TimeUnit::Milliseconds))
                .unwrap(),
            Series::new("a".into(), [1i64, 2])
                .cast(&DataType::List(Box::new(DataType::Int64)))
                .unwrap(),
            // The same bytes the bare binary case is refused for.
            Series::new("a".into(), [&[0xffu8, 0xfe][..], &[0x41][..]]),
        ];
        for inner in inners {
            let nested = StructChunked::from_series("x".into(), 2, [inner].iter())
                .unwrap()
                .into_series();
            cases.push((nested.dtype().clone(), nested.into()));
        }

        for (dtype, column) in cases {
            let cast_works = DataFrame::new(2, vec![column])
                .unwrap()
                .lazy()
                .select([col("x").cast(DataType::String)])
                .collect()
                .is_ok();
            assert_eq!(
                can_read_as_text(&dtype),
                cast_works,
                "{dtype:?}: the predicate and the cast must agree"
            );
        }
    }

    /// Asking for a column the cast would refuse leaves it as it was, rather than
    /// failing the read of every file including the ones that agreed.
    #[test]
    fn a_column_the_cast_refuses_is_read_as_it_was() {
        use polars::prelude::{ParquetWriter, df};

        let dir = tempfile::tempdir().unwrap();
        let mut paths = Vec::new();
        let mut write = |name: &str, mut frame: polars::prelude::DataFrame| {
            let path = dir.path().join(name);
            let f = std::fs::File::create(&path).unwrap();
            ParquetWriter::new(f).finish(&mut frame).unwrap();
            paths.push(path.to_string_lossy().to_string());
        };
        // Bytes that are not text in one file, text in the other.
        write(
            "a.parquet",
            df!("id" => &[0i64, 1], "n" => &[&[0xffu8, 0xfe][..], &[0x41][..]]).unwrap(),
        );
        write("b.parquet", df!("id" => &[2i64], "n" => &["x"]).unwrap());

        let footers: Vec<Option<FileSchema>> = vec![
            file(&[("id", DataType::Int64), ("n", DataType::Binary)], 2),
            file(&[("id", DataType::Int64), ("n", DataType::String)], 1),
        ];
        let dataset = union_file_schemas(&footers, SchemaOrigin::AllFooters(2));
        let drifting = dataset
            .columns
            .iter()
            .find(|column| column.name == "n")
            .unwrap();
        assert!(
            !drifting.can_read_as_text(),
            "so the Notes tab never offers it"
        );

        let drift = ScanDrift::new(&paths, &dataset, &[2, 1]).expect("the files disagree");
        let as_text = [PlSmallStr::from("n")];
        let frame = lenient_scan(&paths, dataset.schema.clone(), None, Some(&drift), &as_text)
            .unwrap()
            .collect()
            .expect("the read still succeeds, which is the point");
        assert_eq!(
            frame
                .column("id")
                .unwrap()
                .i64()
                .unwrap()
                .into_no_null_iter()
                .collect::<Vec<_>>(),
            [0, 1, 2],
            "every file is still read, the agreeing one included"
        );
        assert_ne!(
            frame.column("n").unwrap().dtype(),
            &DataType::String,
            "and the column is as it was, not half-cast"
        );
    }

    /// The type that rules a column out can be one only a *conflicting* file holds.
    ///
    /// The column here is read as an integer, which casts to text perfectly well. It is
    /// the one file storing it as a list that makes the offer impossible — and that
    /// file's cast is the one that would fail, taking the read of the other three with
    /// it. So the answer has to come from every type any file holds, not from the type
    /// the column is read as.
    #[test]
    fn a_type_only_one_file_holds_can_rule_the_column_out() {
        use polars::prelude::{IntoLazy, ParquetWriter, col, df};

        let dir = tempfile::tempdir().unwrap();
        let mut paths = Vec::new();
        let mut write = |name: &str, mut frame: polars::prelude::DataFrame| {
            let path = dir.path().join(name);
            let f = std::fs::File::create(&path).unwrap();
            ParquetWriter::new(f).finish(&mut frame).unwrap();
            paths.push(path.to_string_lossy().to_string());
        };
        write(
            "a.parquet",
            df!("id" => &[0i64, 1, 2], "n" => &[10i64, 20, 30]).unwrap(),
        );
        // `n` as a list here: grouped so the column really is List(Int64) on disk.
        write(
            "b.parquet",
            df!("id" => &[3i64], "n" => &[9i64])
                .unwrap()
                .lazy()
                .group_by([col("id")])
                .agg([col("n")])
                .collect()
                .unwrap(),
        );

        let footers: Vec<Option<FileSchema>> = vec![
            file(&[("id", DataType::Int64), ("n", DataType::Int64)], 3),
            file(
                &[
                    ("id", DataType::Int64),
                    ("n", DataType::List(Box::new(DataType::Int64))),
                ],
                1,
            ),
        ];
        let dataset = union_file_schemas(&footers, SchemaOrigin::AllFooters(2));
        assert_eq!(
            dataset.schema.get("n"),
            Some(&DataType::Int64),
            "read as the integer the three rows have"
        );
        let drifting = dataset
            .columns
            .iter()
            .find(|column| column.name == "n")
            .unwrap();
        assert!(
            can_read_as_text(&drifting.dtype),
            "an integer column casts to text on its own account"
        );
        assert!(
            !drifting.can_read_as_text(),
            "but one file holds a list, and that file's cast is the one that fails"
        );

        let drift = ScanDrift::new(&paths, &dataset, &[3, 1]).expect("the files disagree");
        let as_text = [PlSmallStr::from("n")];
        let frame = lenient_scan(&paths, dataset.schema.clone(), None, Some(&drift), &as_text)
            .unwrap()
            .collect()
            .expect("asking anyway must not cost the read");
        assert_eq!(
            frame
                .column("id")
                .unwrap()
                .i64()
                .unwrap()
                .into_no_null_iter()
                .collect::<Vec<_>>(),
            [0, 1, 2, 3],
            "every file is read, the three that agreed included"
        );
        assert_eq!(
            frame.column("n").unwrap().dtype(),
            &DataType::Int64,
            "and the column is as it was"
        );
    }

    /// `text_schema` spells the named columns as text and moves nothing.
    #[test]
    fn text_schema_respells_without_reordering() {
        let mut schema = Schema::with_capacity(3);
        schema.with_column("a".into(), DataType::Int64);
        schema.with_column("n".into(), DataType::Int64);
        schema.with_column("z".into(), DataType::Float64);
        let schema = Arc::new(schema);

        let text = text_schema(&schema, &[PlSmallStr::from("n")]);
        assert_eq!(
            names(&text),
            ["a", "n", "z"],
            "a column read differently does not move"
        );
        assert_eq!(text.get("n"), Some(&DataType::String));
        assert_eq!(
            text.get("a"),
            Some(&DataType::Int64),
            "nor do its neighbours change"
        );
        assert_eq!(text.get("z"), Some(&DataType::Float64));

        assert!(
            Arc::ptr_eq(&schema, &text_schema(&schema, &[])),
            "asking for nothing is the schema itself"
        );
        assert_eq!(
            names(&text_schema(&schema, &[PlSmallStr::from("ghost")])),
            ["a", "n", "z"],
            "a name the schema does not have adds nothing"
        );
    }

    /// A sampled dataset counts against the footers it read, not against every file.
    ///
    /// `union_sampled` spreads the per-file findings back across the whole list, and it
    /// is tempting to spread the totals with them. It must not: datui opened a few
    /// thousand footers out of a few hundred thousand files, and every count it states
    /// — the denominator the notes divide by, how many files hold no rows — is a count
    /// of what it opened. A total over the full list would be a claim about files it
    /// never looked at.
    #[test]
    fn a_sampled_dataset_counts_what_it_read_and_not_what_it_did_not() {
        let footers = vec![
            file(&[("id", DataType::Int64)], 0),
            file(&[("id", DataType::Int64), ("x", DataType::String)], 5),
            None,
        ];
        // Three footers read, spread across five hundred files.
        let read = [0usize, 250, 499];
        let union = union_sampled(500, &read, &footers);

        assert_eq!(
            union.files, 3,
            "the population is the footers read, not the files there are"
        );
        assert_eq!(union.empty_files, 1, "one of the three held nothing");
        assert_eq!(
            union.origin,
            SchemaOrigin::FooterSample {
                read: 3,
                total: 500
            }
        );
        assert_eq!(
            union.unreadable,
            [499],
            "and the footer that would not parse is named by its place among the files"
        );
        // The per-file findings, though, are spread to the full length: the scan
        // indexes them by file, and it reads all five hundred.
        assert_eq!(union.file_group.len(), 500);
        assert_eq!(union.omitted.len(), 500);
    }

    /// The row-group note fires on the middle size, and only past the threshold.
    ///
    /// Sizes rather than schemas, so it does not go through `Shape`: what decides this
    /// note is a list of numbers, and the interesting cases are all about which number
    /// the middle is.
    #[test]
    fn row_groups_are_noted_by_their_middle_size_and_only_when_it_is_large() {
        const MIB: usize = 1024 * 1024;
        let note = |groups: &[&[usize]]| -> Option<String> {
            let files: Vec<Option<FileSchema>> = groups
                .iter()
                .map(|sizes| {
                    Some(FileSchema {
                        schema: Arc::new(Schema::with_capacity(0)),
                        rows: 1,
                        file_bytes: 0,
                        row_group_bytes: sizes.to_vec(),
                    })
                })
                .collect();
            let dataset = union_file_schemas(&files, SchemaOrigin::AllFooters(files.len()));
            crate::notes::from_dataset(&dataset)
                .into_iter()
                .find(|note| note.summary.starts_with("the middle row group"))
                .map(|note| note.summary)
        };

        assert_eq!(note(&[&[MIB], &[2 * MIB]]), None, "ordinary row groups");
        assert_eq!(
            note(&[&[64 * MIB]]),
            None,
            "the threshold itself is not past it"
        );
        assert_eq!(
            note(&[&[65 * MIB]]).as_deref(),
            Some("the middle row group is 65.0 MiB, and rows are read a row group at a time"),
        );
        assert_eq!(
            note(&[&[MIB, MIB, 4096 * MIB]]),
            None,
            "one huge row group among small ones does not describe the dataset"
        );
        assert_eq!(
            note(&[&[100 * MIB, 100 * MIB], &[MIB]]).as_deref(),
            Some("the middle row group is 100.0 MiB, and rows are read a row group at a time"),
            "the middle of every row group of every file, not the middle of the files"
        );
        assert_eq!(
            note(&[&[MIB], &[100 * MIB, 100 * MIB]]).as_deref(),
            Some("the middle row group is 100.0 MiB, and rows are read a row group at a time"),
            "including when the large ones are not in the first file"
        );
        // Row groups arrive in file order, which is no order at all by size: a middle
        // partition rewritten by another job puts a big one between two small ones.
        assert_eq!(
            note(&[&[100 * MIB], &[MIB], &[100 * MIB]]).as_deref(),
            Some("the middle row group is 100.0 MiB, and rows are read a row group at a time"),
            "and when they arrive out of order"
        );
        assert_eq!(
            note(&[&[MIB], &[100 * MIB], &[MIB]]),
            None,
            "which cuts both ways: one big group between two small ones is not the middle"
        );
        assert_eq!(note(&[&[]]), None, "a file with no row groups says nothing");
        // An even count takes the lower of the middle two, which is the reading that
        // errs towards saying nothing.
        assert_eq!(
            note(&[&[64 * MIB, 65 * MIB]]),
            None,
            "two row groups either side of the line: the lower one decides"
        );
        assert_eq!(
            note(&[&[65 * MIB, 66 * MIB]]).as_deref(),
            Some("the middle row group is 65.0 MiB, and rows are read a row group at a time"),
            "and when it decides the other way it is still the lower one"
        );
    }

    /// The sizes come off a real Parquet footer, and they are the compressed ones.
    ///
    /// Compressed, because that is what crosses the wire; the other number the footer
    /// offers is the size once decoded. Telling them apart takes data that does not
    /// compress to nothing: twenty thousand distinct strings compress to about 30 KiB
    /// from about 4 MiB decoded, where a column of one repeated integer goes the other
    /// way — the dictionary makes the decoded figure the *smaller* of the two, and a
    /// test built on that pins nothing.
    #[test]
    fn a_real_footer_reports_the_compressed_size_of_each_row_group() {
        use polars::prelude::{ParquetWriter, df};

        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<String> = (0..20_000)
            .map(|i| format!("{i:0>6}{}", "abcdefghij".repeat(19)))
            .collect();
        let mut frame = df!("s" => rows).unwrap();
        let file = std::fs::File::create(dir.path().join("wide.parquet")).unwrap();
        ParquetWriter::new(file)
            .with_row_group_size(Some(20_000))
            .finish(&mut frame)
            .unwrap();

        let (files, read, footers) =
            crate::widgets::datatable::DataTableState::footers_of_parquet_dir(dir.path());
        assert_eq!((files.len(), read.len()), (1, 1));
        let footer = footers[0].as_ref().expect("the footer reads");
        assert_eq!(footer.rows, 20_000);
        assert_eq!(footer.row_group_bytes.len(), 1, "one row group");

        // The file's own size comes from the same read, and is the size on disk: the
        // compressed row group plus the footer and header around it, so larger than
        // the group and far smaller than the decoded data.
        let on_disk = std::fs::metadata(dir.path().join("wide.parquet"))
            .unwrap()
            .len();
        assert_eq!(
            footer.file_bytes as u64, on_disk,
            "the file's size, as the filesystem reports it"
        );

        let size = footer.row_group_bytes[0];
        assert!(size > 0, "a size is reported");
        assert!(
            size < 1_000_000,
            "and it is the compressed size: 20,000 distinct strings of 200 characters \
             are about 4 MiB decoded and a small fraction of that on disk, so {size} \
             bytes is the decoded figure"
        );
    }

    /// Many small files is two conditions, and both have to hold.
    #[test]
    fn many_files_are_noted_only_when_they_are_also_small() {
        const KIB: usize = 1024;
        const MIB: usize = 1024 * KIB;
        // `sizes` is the shape of the footers read, repeated to fill `read` of them:
        // the note says how many were read, so the fixture has to have that many.
        let note = |files: usize, read: usize, sizes: &[usize]| -> Option<String> {
            let footers: Vec<Option<FileSchema>> = sizes
                .iter()
                .cycle()
                .take(if sizes.is_empty() { 0 } else { read })
                .map(|bytes| {
                    Some(FileSchema {
                        schema: Arc::new(Schema::with_capacity(0)),
                        rows: 1,
                        file_bytes: *bytes,
                        row_group_bytes: Vec::new(),
                    })
                })
                .collect();
            let origin = if read == files {
                SchemaOrigin::AllFooters(files)
            } else {
                SchemaOrigin::FooterSample { read, total: files }
            };
            crate::notes::from_dataset(&union_file_schemas(&footers, origin))
                .into_iter()
                .find(|note| note.summary.starts_with("there are"))
                .map(|note| note.summary)
        };

        assert_eq!(
            note(10_000, 10_000, &[40 * KIB]),
            None,
            "a year of hourly partitions, and more, is an ordinary shape"
        );
        assert_eq!(
            note(10_001, 10_001, &[40 * KIB]).as_deref(),
            Some(
                "there are 10,001 files and the middle one is 40.0 KiB; each was opened for its footer before a row was"
            ),
            "one more is not"
        );
        assert_eq!(
            note(50_000, 50_000, &[MIB]),
            None,
            "a megabyte is not small by this measure"
        );
        assert!(
            note(50_000, 50_000, &[MIB - 1]).is_some(),
            "a byte under it is"
        );
        assert_eq!(
            note(50_000, 50_000, &[40 * KIB, 40 * KIB, 900 * MIB]).as_deref(),
            Some(
                "there are 50,000 files and the middle one is 40.0 KiB; each was opened for its footer before a row was"
            ),
            "a large minority does not move the middle"
        );
        // Sampled: the count is every file the listing found, the middle size is over
        // the footers datui opened, and the sentence names both rather than leaving
        // the middle to read as a fact about all of them.
        assert_eq!(
            note(500_000, 2, &[40 * KIB, 40 * KIB]).as_deref(),
            Some(
                "there are 500,000 files and the middle one is 40.0 KiB; 2 were opened for their footers before a row was"
            ),
            "the count is the listing's; the footers read are their own number"
        );
        assert_eq!(note(50_000, 0, &[]), None, "no footer read, nothing to say");

        // The scope line under a sampled dataset says what was looked at, which is what
        // stops the middle size reading as a fact about half a million files.
        let sampled = union_file_schemas(
            &[
                Some(FileSchema {
                    schema: Arc::new(Schema::with_capacity(0)),
                    rows: 1,
                    file_bytes: 40 * KIB,
                    row_group_bytes: Vec::new(),
                }),
                Some(FileSchema {
                    schema: Arc::new(Schema::with_capacity(0)),
                    rows: 1,
                    file_bytes: 40 * KIB,
                    row_group_bytes: Vec::new(),
                }),
            ],
            SchemaOrigin::FooterSample {
                read: 2,
                total: 500_000,
            },
        );
        let sampled_note = crate::notes::from_dataset(&sampled)
            .into_iter()
            .find(|note| note.summary.starts_with("there are"))
            .expect("the note is made");
        assert_eq!(sampled_note.scope, "in 2 of 500,000 footers (sample)");
        assert_eq!(
            note(50_000, 2, &[0, 0]),
            None,
            "and a size of nothing means the size is not known, not that it is small"
        );
    }

    /// The keys a path partitions by: a set, sorted, with the file's own name never
    /// among them.
    ///
    /// A set because hive columns are matched by name — `y=1/m=1` and `m=2/y=2`
    /// partition by the same two things, and a dataset that mixes the two orders reads
    /// perfectly well. Sorted so the two spell alike, and deduplicated so a tree that
    /// repeats a key is one thing rather than two.
    #[test]
    fn partition_keys_are_the_key_equals_segments_above_the_file() {
        let keys = |path: &str| partition_keys_of(path);
        assert_eq!(keys("data/date=2024-01-01/a.parquet"), ["date"]);
        assert_eq!(keys("data/y=2024/m=05/a.parquet"), ["m", "y"]);
        assert_eq!(
            keys("data/m=05/y=2024/a.parquet"),
            keys("data/y=2024/m=05/a.parquet"),
            "the same two partitions, written in two orders"
        );
        assert_eq!(
            keys("data/x=1/x=2/a.parquet"),
            ["x"],
            "and a key repeated down the tree is one key"
        );
        assert_eq!(keys("data/a.parquet"), Vec::<String>::new());
        assert_eq!(
            keys("data/x=1/2024=05.parquet"),
            ["x"],
            "the file's own name is not a partition, whatever it looks like"
        );
        assert_eq!(
            keys("data/=2024/a.parquet"),
            Vec::<String>::new(),
            "nor is a segment with nothing before the equals"
        );
        // A backslash separates on Windows and is an ordinary character in a Linux
        // file name. Splitting on it everywhere would break a legitimate name and
        // invent a layout difference out of one directory, which would fire this note
        // on a dataset whose folders agree perfectly.
        #[cfg(windows)]
        assert_eq!(
            keys(r"data\date=2024-01-01\a.parquet"),
            ["date"],
            "a path written the other way round is the same path"
        );
        #[cfg(not(windows))]
        assert_eq!(
            keys(r"data/we\ird=1/f.parquet"),
            ["we\\ird"],
            "a backslash here is part of the name, not a separator"
        );
        #[cfg(not(windows))]
        assert_eq!(
            keys(r"data/x=1\y=2/f.parquet"),
            ["x"],
            "so one directory is one partition, however it is spelled"
        );
    }

    /// A dataset whose folders do not all partition by the same keys.
    ///
    /// The note says the shape and claims nothing about what it costs: a rename that
    /// stops the dataset opening, and one stray unpartitioned file that turns hive
    /// reading off and leaves the same folders readable, look identical from here.
    #[test]
    fn folders_that_partition_differently_are_counted_each_way() {
        let note = |root: &str, paths: &[&str]| -> Option<crate::notes::Note> {
            let footers = vec![
                Some(FileSchema {
                    schema: Arc::new(Schema::with_capacity(0)),
                    rows: 1,
                    file_bytes: 1,
                    row_group_bytes: Vec::new(),
                });
                paths.len()
            ];
            let owned: Vec<String> = paths.iter().map(|p| p.to_string()).collect();
            let dataset = union_file_schemas(&footers, SchemaOrigin::AllFooters(paths.len()))
                .with_partition_layouts(root, &owned);
            crate::notes::from_dataset(&dataset)
                .into_iter()
                .find(|note| note.summary.contains("partition by the same keys"))
        };

        assert_eq!(
            note("d", &["d/date=1/a.parquet", "d/date=2/b.parquet"]),
            None,
            "folders that agree have nothing to say"
        );
        assert_eq!(
            note("d", &["d/a.parquet", "d/b.parquet"]),
            None,
            "nor has a dataset with no partitions at all"
        );
        assert_eq!(
            note("d", &["d/y=1/m=1/a.parquet", "d/m=2/y=2/b.parquet"]),
            None,
            "nor two orders of the same two keys: hive matches columns by name, so \
             that dataset reads perfectly well and has nothing in dispute"
        );
        assert_eq!(
            note(
                "d/run=7",
                &["d/run=7/loose.parquet", "d/run=7/date=1/a.parquet"]
            ),
            None,
            "a key=value folder above the dataset as it was opened is not one of the \
             things its folders disagree about — and these two files are where that \
             matters, since counting `run` would make the one without a key of its \
             own a second layout"
        );
        assert_eq!(
            note(
                "s3://b//data/",
                &["s3://b/data/date=1/a.parquet", "s3://b/data/dt=2/b.parquet"]
            ),
            None,
            "and a path the root is not a prefix of — a typed URL with a doubled \
             slash rebuilds without it — is one this cannot place, so it is left out \
             rather than read from the top"
        );

        let renamed = note(
            "d",
            &[
                "d/date=1/a.parquet",
                "d/date=2/b.parquet",
                "d/date=3/c.parquet",
                "d/dt=4/e.parquet",
            ],
        )
        .expect("the folders disagree");
        assert_eq!(
            renamed.summary,
            "the folders do not all partition by the same keys: 3 files by date, \
             1 file by dt"
        );
        assert_eq!(
            renamed.scope, "in the names of 4 files",
            "read off every name, not off the footers datui opened"
        );

        // A file with no partition at all has no keys to disagree about, so it is no
        // layout — but it is still a name that was read, and the scope counts it.
        let loose = note(
            "d",
            &[
                "d/y=1/m=1/a.parquet",
                "d/y=1/m=2/b.parquet",
                "d/date=3/c.parquet",
                "d/loose.parquet",
            ],
        )
        .expect("the folders disagree");
        assert_eq!(
            loose.summary,
            "the folders do not all partition by the same keys: 2 files by m/y, \
             1 file by date"
        );
        assert_eq!(
            loose.scope, "in the names of 4 files",
            "the unpartitioned file is one of the names read"
        );
    }

    /// The commonest layout is named first, and past two the rest are counted.
    #[test]
    fn the_layouts_a_note_names_are_the_commonest_of_them() {
        let layouts = |paths: &[&str]| -> Vec<(Vec<String>, usize)> {
            let owned: Vec<String> = paths.iter().map(|p| p.to_string()).collect();
            union_file_schemas(&[], SchemaOrigin::AllFooters(0))
                .with_partition_layouts("d", &owned)
                .partition_layouts
        };
        let note = |paths: &[&str]| -> String {
            let footers = vec![
                Some(FileSchema {
                    schema: Arc::new(Schema::with_capacity(0)),
                    rows: 1,
                    file_bytes: 1,
                    row_group_bytes: Vec::new(),
                });
                paths.len()
            ];
            let owned: Vec<String> = paths.iter().map(|p| p.to_string()).collect();
            let dataset = union_file_schemas(&footers, SchemaOrigin::AllFooters(paths.len()))
                .with_partition_layouts("d", &owned);
            crate::notes::from_dataset(&dataset)
                .into_iter()
                .find(|note| note.summary.contains("partition by the same keys"))
                .expect("the folders disagree")
                .summary
        };

        // Asserted on the layouts themselves, not on the note: a HashMap hands them
        // back in no order at all, so a note that happened to read correctly would
        // leave the ordering untested nine runs in ten.
        assert_eq!(
            layouts(&[
                "d/zzz=1/b.parquet",
                "d/aaa=1/a.parquet",
                "d/zzz=2/c.parquet",
                "d/zzz=3/e.parquet",
            ]),
            vec![(vec!["zzz".to_string()], 3), (vec!["aaa".to_string()], 1)],
            "commonest first, though the rare one sorts first and arrived first"
        );
        assert_eq!(
            layouts(&[
                "d/zz=1/a.parquet",
                "d/aa=1/b.parquet",
                "d/mm=1/c.parquet",
                "d/qq=1/e.parquet"
            ]),
            vec![
                (vec!["aa".to_string()], 1),
                (vec!["mm".to_string()], 1),
                (vec!["qq".to_string()], 1),
                (vec!["zz".to_string()], 1)
            ],
            "and equally common ones by their keys, so the same dataset reads the \
             same way every time it is opened"
        );

        assert_eq!(
            note(&[
                "d/aa=1/a.parquet",
                "d/bb=1/b.parquet",
                "d/cc=1/c.parquet",
                "d/dd=1/e.parquet",
            ]),
            "the folders do not all partition by the same keys: 1 file by aa, \
             1 file by bb, 2 files by 2 other ways"
        );
        assert_eq!(
            note(&["d/aa=1/a.parquet", "d/bb=1/b.parquet", "d/cc=1/c.parquet"]),
            "the folders do not all partition by the same keys: 1 file by aa, \
             1 file by bb, 1 file by 1 other way",
            "and one of them is one way, not one ways"
        );

        // Past the layouts worth remembering, the tail is still counted in full: a
        // note that says "and 62 other ways" of a hundred would not add up against
        // its own scope line.
        let many: Vec<String> = (0..100)
            .map(|i| format!("d/k{i:0>3}=1/f.parquet"))
            .collect();
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        assert_eq!(
            note(&many),
            "the folders do not all partition by the same keys: 1 file by k000, \
             1 file by k001, 98 files by 98 other ways"
        );
        let owned: Vec<String> = many.iter().map(|p| p.to_string()).collect();
        let dataset = union_file_schemas(&[], SchemaOrigin::AllFooters(0))
            .with_partition_layouts("d", &owned);
        assert!(
            dataset.partition_layouts.len() <= 64,
            "and it is not holding a hundred of them to say so: {}",
            dataset.partition_layouts.len()
        );
    }

    /// The counter says nothing until a pass begins, and nothing again once it ends.
    ///
    /// Nothing-when-done is the half that matters: a count left on screen after the
    /// footers have landed is a wait the user is not actually having.
    #[test]
    fn the_footer_count_speaks_only_while_a_pass_is_running() {
        let progress = FooterProgress::default();
        assert_eq!(progress.reading(), None, "nothing has begun");

        progress.begin(3);
        assert_eq!(progress.reading(), Some((0, 3)), "none read yet");
        progress.advance();
        progress.advance();
        assert_eq!(progress.reading(), Some((2, 3)));

        progress.done();
        assert_eq!(progress.reading(), None, "and nothing once it has landed");

        // A second pass starts from nothing rather than from the first one's count.
        progress.begin(2);
        assert_eq!(progress.reading(), Some((0, 2)));
    }

    /// More advances than footers cannot make the count overtake the total.
    ///
    /// No caller can reach it today: a pass is begun before its readers are spawned
    /// and is over before the next one begins, and every open takes a counter of its
    /// own. The clamp is for the wiring that comes after this one — "reading 4 of 3
    /// footers" is the sort of nonsense that makes a user distrust the rest of the
    /// screen. It does mean a future miswiring shows as a count stopped at N of N
    /// rather than as an obvious absurdity, which is the price of not showing the
    /// absurdity.
    #[test]
    fn the_footer_count_never_passes_its_total() {
        let progress = FooterProgress::default();
        progress.begin(2);
        for _ in 0..5 {
            progress.advance();
        }
        assert_eq!(progress.reading(), Some((2, 2)));
    }

    /// A pass that panics still says it has finished.
    ///
    /// The counter outlives the pass — the render holds it — so a pass that stopped
    /// without saying so would leave a count on screen for as long as anyone looked,
    /// which is the one state this feature exists to prevent.
    #[test]
    fn a_pass_that_panics_still_says_it_has_finished() {
        let progress = FooterProgress::default();
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let pass = progress.pass(3);
            pass.advance();
            panic!("a footer reader gave up");
        }));
        assert!(caught.is_err(), "the panic happened");
        assert_eq!(
            progress.reading(),
            None,
            "and the count went with it rather than sitting there"
        );
        assert_eq!(
            progress.last_pass().read,
            1,
            "with what it managed still readable"
        );
    }

    /// Files merely missing a column must not split the scan.
    ///
    /// Splitting is only needed to leave a column out of a file that holds it in
    /// another type. When a column is simply absent the read is already lenient, so a
    /// dataset whose files alternate between having it and not — the worst case for
    /// run-splitting — must still be one scan.
    #[test]
    fn absent_columns_alone_never_split_the_scan() {
        let files = 64;
        let per_file: Vec<Option<FileSchema>> = (0..files)
            .map(|i| {
                let mut s = Schema::with_capacity(2);
                s.with_column("id".into(), DataType::Int64);
                if i % 2 == 1 {
                    s.with_column("extra".into(), DataType::String);
                }
                Some(FileSchema {
                    schema: Arc::new(s),
                    rows: 1,
                    file_bytes: 0,
                    row_group_bytes: Vec::new(),
                })
            })
            .collect();
        let paths: Vec<String> = (0..files).map(|i| format!("part-{i:05}.parquet")).collect();
        let read: Vec<usize> = (0..files).collect();
        let dataset = union_sampled(files, &read, &per_file);
        let rows = vec![1usize; files];
        let drift = ScanDrift::new(&paths, &dataset, &rows).expect("this dataset drifts");
        assert!(dataset.drifts());
        assert_eq!(
            runs_of(&paths, &drift),
            1,
            "absent columns need no split, however they alternate"
        );

        // A type conflict does need one, and only around the files that have it.
        let mut with_conflict = per_file.clone();
        let mut odd = Schema::with_capacity(2);
        odd.with_column("id".into(), DataType::String);
        with_conflict[7] = Some(FileSchema {
            schema: Arc::new(odd),
            rows: 1,
            file_bytes: 0,
            row_group_bytes: Vec::new(),
        });
        let dataset = union_sampled(files, &read, &with_conflict);
        let drift = ScanDrift::new(&paths, &dataset, &rows).unwrap();
        assert_eq!(runs_of(&paths, &drift), 3, "before it, it, and after it");
    }

    /// How many separate scans `lenient_scan` would build for these paths.
    fn runs_of(paths: &[String], drift: &ScanDrift) -> usize {
        let mut runs = 1;
        for pair in paths.windows(2) {
            if drift.unread(&pair[0]) != drift.unread(&pair[1]) {
                runs += 1;
            }
        }
        runs
    }

    #[test]
    fn a_column_only_a_middle_file_has_is_kept() {
        let files = [
            file(&[("id", DataType::Int64)], 10),
            file(&[("id", DataType::Int64), ("oops", DataType::String)], 10),
            file(&[("id", DataType::Int64)], 10),
        ];
        let union = union(&files);
        assert_eq!(names(&union.schema), ["id", "oops"]);
        let oops = union.columns.iter().find(|c| c.name == "oops").unwrap();
        assert_eq!(oops.present_in, 1);
    }

    #[test]
    fn the_newest_files_order_leads_and_older_columns_follow() {
        let files = [
            file(&[("a", DataType::Int64), ("gone", DataType::Int64)], 1),
            file(&[("b", DataType::Int64), ("a", DataType::Int64)], 1),
        ];
        assert_eq!(names(&union(&files).schema), ["b", "a", "gone"]);
    }

    #[test]
    fn integer_widths_widen_losslessly() {
        let files = [
            file(&[("n", DataType::Int32)], 100),
            file(&[("n", DataType::Int64)], 1),
        ];
        let union = union(&files);
        assert_eq!(union.schema.get("n"), Some(&DataType::Int64));
        assert!(union.columns[0].widened);
        assert_eq!(union.columns[0].conflicting_files, 0);
        assert!(union.omitted.iter().all(|o| o.is_empty()));
    }

    #[test]
    fn an_integer_and_a_float_meet_at_float64() {
        let files = [
            file(&[("n", DataType::Int32)], 1),
            file(&[("n", DataType::Float32)], 1),
        ];
        assert_eq!(union(&files).schema.get("n"), Some(&DataType::Float64));
    }

    #[test]
    fn datetime_units_widen_to_the_finer_one() {
        let ms = DataType::Datetime(TimeUnit::Milliseconds, None);
        let ns = DataType::Datetime(TimeUnit::Nanoseconds, None);
        let files = [file(&[("t", ms)], 1), file(&[("t", ns.clone())], 1)];
        assert_eq!(union(&files).schema.get("t"), Some(&ns));
    }

    #[test]
    fn a_struct_has_every_field_either_file_has() {
        let old = DataType::Struct(vec![Field::new("a".into(), DataType::Int32)]);
        let new = DataType::Struct(vec![
            Field::new("a".into(), DataType::Int64),
            Field::new("b".into(), DataType::String),
        ]);
        let files = [file(&[("s", old)], 1), file(&[("s", new.clone())], 1)];
        assert_eq!(union(&files).schema.get("s"), Some(&new));
    }

    #[test]
    fn a_type_conflict_goes_to_the_majority_of_rows() {
        let files = [
            file(&[("price", DataType::String)], 10),
            file(&[("price", DataType::Int64)], 90),
        ];
        let union = union(&files);
        assert_eq!(union.schema.get("price"), Some(&DataType::Int64));
        assert_eq!(union.columns[0].conflicting_files, 1);
        assert_eq!(union.columns[0].conflicting_types, [DataType::String]);
        assert_eq!(omitted_names(&union, 0), ["price"]);
        assert!(union.omitted[1].is_empty());
    }

    #[test]
    fn the_majority_can_be_the_text_files() {
        let files = [
            file(&[("price", DataType::String)], 90),
            file(&[("price", DataType::Int64)], 10),
        ];
        let union = union(&files);
        assert_eq!(union.schema.get("price"), Some(&DataType::String));
        assert_eq!(omitted_names(&union, 1), ["price"]);
    }

    #[test]
    fn a_type_that_covers_more_files_wins_over_one_that_covers_none_extra() {
        // Float64 is in no file, but reads both numeric ones; the text file loses.
        let files = [
            file(&[("n", DataType::Int32)], 30),
            file(&[("n", DataType::Float32)], 30),
            file(&[("n", DataType::String)], 50),
        ];
        let union = union(&files);
        assert_eq!(union.schema.get("n"), Some(&DataType::Float64));
        assert_eq!(omitted_names(&union, 2), ["n"]);
    }

    #[test]
    fn names_differing_only_by_case_stay_two_columns() {
        let files = [file(
            &[("Price", DataType::Int64), ("price", DataType::Int64)],
            1,
        )];
        assert_eq!(names(&union(&files).schema), ["Price", "price"]);
    }

    #[test]
    fn an_unreadable_footer_is_recorded_and_left_out() {
        let files = [
            file(&[("id", DataType::Int64)], 1),
            None,
            file(&[("id", DataType::Int64), ("late", DataType::Int64)], 1),
        ];
        let union = union(&files);
        assert_eq!(union.unreadable, [1]);
        assert_eq!(names(&union.schema), ["id", "late"]);
        assert!(union.omitted[1].is_empty());
    }

    #[test]
    fn a_column_of_nulls_takes_the_other_files_type() {
        let files = [
            file(&[("x", DataType::Null)], 1),
            file(&[("x", DataType::Int64)], 1),
        ];
        let union = union(&files);
        assert_eq!(union.schema.get("x"), Some(&DataType::Int64));
        assert_eq!(union.columns[0].conflicting_files, 0);
    }

    #[test]
    fn unsigned_and_signed_meet_in_a_wider_signed_type() {
        assert_eq!(
            widen(&DataType::UInt32, &DataType::Int32),
            Some(DataType::Int64)
        );
        assert_eq!(widen(&DataType::UInt64, &DataType::Int64), None);
    }

    #[test]
    fn origins_read_as_sentences() {
        assert_eq!(
            SchemaOrigin::AllFooters(6541).to_string(),
            "all 6,541 footers"
        );
        assert_eq!(
            SchemaOrigin::FooterSample {
                read: 5000,
                total: 200_000
            }
            .to_string(),
            "5,000 of 200,000 footers (sample)"
        );
    }

    #[test]
    fn a_sample_spans_the_files_and_keeps_the_first_and_newest() {
        assert_eq!(footers_to_read(3), [0, 1, 2]);
        assert_eq!(footers_to_read(MAX_FOOTER_READS).len(), MAX_FOOTER_READS);
        let sample = footers_to_read(MAX_FOOTER_READS * 10);
        assert_eq!(sample.len(), MAX_FOOTER_READS);
        assert_eq!(sample.first(), Some(&0));
        assert_eq!(sample.last(), Some(&(MAX_FOOTER_READS * 10 - 1)));
        assert!(sample.windows(2).all(|w| w[0] < w[1]), "ascending");
    }

    /// The scan's cast policy has no way to read either of these into the other, so
    /// they must stay conflicts and be omitted rather than widened into a type the
    /// read would then fail on.
    #[test]
    fn types_the_scan_cannot_cast_are_not_widened() {
        let ms = DataType::Duration(TimeUnit::Milliseconds);
        let us = DataType::Duration(TimeUnit::Microseconds);
        assert_eq!(widen(&ms, &us), None);
        assert_eq!(widen(&DataType::Binary, &DataType::String), None);
        assert_eq!(widen(&DataType::Date, &ms), None);
    }

    #[test]
    fn drifting_counts_against_the_files_read_not_the_busiest_column() {
        // No column is in both files; both are drift.
        let files = [
            file(&[("a", DataType::Int64)], 1),
            file(&[("b", DataType::Int64)], 1),
        ];
        let union = union(&files);
        let drifting: Vec<_> = union.drifting().map(|c| c.name.to_string()).collect();
        assert_eq!(drifting, ["b", "a"]);
    }

    #[test]
    fn drifting_names_only_the_columns_worth_a_note() {
        let files = [
            file(&[("id", DataType::Int64)], 1),
            file(&[("id", DataType::Int64), ("oops", DataType::String)], 1),
        ];
        let union = union(&files);
        let drifting: Vec<_> = union.drifting().map(|c| c.name.to_string()).collect();
        assert_eq!(drifting, ["oops"]);
    }
}
