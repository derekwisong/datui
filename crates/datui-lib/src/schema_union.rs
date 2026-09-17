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

use std::collections::HashMap;
use std::sync::Arc;

use polars::prelude::{DataType, Field, PlSmallStr, Schema, TimeUnit};

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
    /// Past `max_footer_reads`: a spread sample of the files.
    FooterSample { read: usize, total: usize },
    /// One file, as before this module existed (`single_spine_schema = false`, or a
    /// dataset of one file).
    OneFile,
    /// A glob, left to Polars to expand.
    PolarsScan,
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
            SchemaOrigin::OneFile => write!(f, "one file"),
            SchemaOrigin::PolarsScan => write!(f, "Polars scan"),
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
    /// Per file, in the order given, the columns not read from it. Empty for a file
    /// whose types all fit.
    pub omitted: Vec<Vec<PlSmallStr>>,
    /// Files whose footer could not be read, by index into the files given.
    pub unreadable: Vec<usize>,
    pub origin: SchemaOrigin,
}

impl DatasetSchema {
    /// Columns that are not in every file, or whose type had to give way.
    pub fn drifting(&self) -> impl Iterator<Item = &ColumnDrift> {
        let files = self.columns.iter().map(|c| c.present_in).max().unwrap_or(0);
        self.columns.iter().filter(move |c| !c.is_uniform(files))
    }
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

    let omitted = files
        .iter()
        .map(|file| {
            let Some(file) = file else {
                return Vec::new();
            };
            file.schema
                .iter()
                .filter(|(name, dtype)| schema.get(name).is_some_and(|target| !fits(dtype, target)))
                .map(|(name, _)| name.clone())
                .collect()
        })
        .collect();

    DatasetSchema {
        schema: Arc::new(schema),
        columns,
        omitted,
        unreadable,
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
        (String, Categorical(_, _) | Enum(_, _)) | (Categorical(_, _) | Enum(_, _), String) => {
            Some(String)
        }
        _ if a.is_integer() && b.is_integer() => widen_integers(a, b),
        _ if (a.is_integer() || a.is_float()) && (b.is_integer() || b.is_float()) => Some(Float64),
        (Datetime(a_unit, a_zone), Datetime(b_unit, b_zone)) if a_zone == b_zone => {
            Some(Datetime(finer_unit(*a_unit, *b_unit), a_zone.clone()))
        }
        (Duration(a_unit), Duration(b_unit)) => Some(Duration(finer_unit(*a_unit, *b_unit))),
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

    fn names(schema: &Schema) -> Vec<String> {
        schema.iter_names().map(|n| n.to_string()).collect()
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
        assert_eq!(union.omitted[0], ["price"]);
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
        assert_eq!(union.omitted[1], ["price"]);
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
        assert_eq!(union.omitted[2], ["n"]);
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
        assert_eq!(SchemaOrigin::OneFile.to_string(), "one file");
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
