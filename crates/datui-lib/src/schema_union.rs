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

use polars::prelude::{
    DataType, Field, LazyFrame, PlRefPath, PlSmallStr, PolarsResult, Schema, TimeUnit, UnionArgs,
    concat,
};

/// What one file's footer said, short of the data.
#[derive(Debug, Clone)]
pub struct FileSchema {
    pub schema: Arc<Schema>,
    pub rows: usize,
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
    }
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
