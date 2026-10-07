//! What datui noticed about a dataset while doing what it was already doing. A note
//! never costs its own request or scan (all come from footers the schema and count
//! needed), is never styled as an alarm, and states its basis, so "in 1 of 3 files" is
//! never read as a claim about files not looked at. One claim per note, and one
//! function (`out_of`) deciding the only ratio any note states.

use crate::formats::schema_union::{
    ColumnDrift, ColumnRange, DatasetSchema, SchemaOrigin, SkippedFiles,
};
use crate::numfmt::group_chrome;
use polars::prelude::{DataType, PlSmallStr};

/// One thing datui noticed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
    /// The one line shown in the panel.
    pub summary: String,
    /// What the note is based on, so its reach is never overstated.
    pub scope: String,
    /// The column datui can offer to read as text, when this note is about one and that
    /// would work; `None` otherwise. A name, so the panel never parses prose.
    pub read_as_text: Option<PlSmallStr>,
    /// How many files this note counts as passed over, when it says a mixed directory was
    /// read as its commonest format; `None` otherwise. A number, so [`merged`] can subtract
    /// it from the footer pass's tally without parsing prose.
    pub passed_over: Option<usize>,
}

/// Whether a dataset's schema came from a sample of its files.
fn sampled(dataset: &DatasetSchema) -> bool {
    matches!(dataset.origin, SchemaOrigin::FooterSample { .. })
}

/// How many, in the noun that is true of what datui looked at: files when it read
/// every one, footers when it read only a sample of them.
fn how_many(dataset: &DatasetSchema, n: usize) -> String {
    let noun = match (sampled(dataset), n) {
        (false, 1) => "file",
        (false, _) => "files",
        (true, 1) => "footer",
        (true, _) => "footers",
    };
    format!("{} {noun}", group_chrome(n))
}

/// The denominator of the dataset's one ratio: files read and parsed. "files" only when
/// every file was read and parsed; otherwise saying so would claim more than was seen.
fn out_of(dataset: &DatasetSchema) -> String {
    let readable = dataset.files.saturating_sub(dataset.unreadable.len());
    match (sampled(dataset), dataset.unreadable.is_empty()) {
        (false, true) => how_many(dataset, readable),
        (false, false) => format!("the {} that could be read", how_many(dataset, readable)),
        (true, true) => format!("the {} read", how_many(dataset, readable)),
        (true, false) => format!("the {} that could be read", how_many(dataset, readable)),
    }
}

/// Names for a set of types that tell them apart: names escalate in detail (to
/// `Debug`) until distinct, since structs all print `struct[1]` and enums
/// `Enum([...])`.
fn distinct_names(types: &[&DataType]) -> Vec<String> {
    let collides = |names: &[String]| {
        names
            .iter()
            .enumerate()
            .any(|(i, name)| names[i + 1..].contains(name))
    };
    // `Display` is how the Schema tab names a type, and it carries the unit, the
    // precision and the time zone that the table header's one word drops.
    let shown: Vec<String> = types.iter().map(|t| format!("{t}")).collect();
    if !collides(&shown) {
        return shown;
    }
    // Two structs are both `struct[1]`, and only the fields tell them apart.
    let spelled: Vec<String> = types.iter().map(|t| format!("{t:?}")).collect();
    if !collides(&spelled) {
        return spelled;
    }
    // Enums and global Categoricals can collide even in full form: number only those that
    // collide.
    let mut seen: Vec<&String> = Vec::new();
    spelled
        .iter()
        .map(|name| {
            if spelled.iter().filter(|other| *other == name).count() > 1 {
                seen.push(name);
                format!("{name} #{}", seen.iter().filter(|s| **s == name).count())
            } else {
                name.clone()
            }
        })
        .collect()
}

/// What the footers said, as notes. Empty when every file agrees, which is the common
/// case and the one where there is nothing to say.
pub fn from_dataset(dataset: &DatasetSchema) -> Vec<Note> {
    let scope = format!("in {}", dataset.origin);
    let readable = dataset.files.saturating_sub(dataset.unreadable.len());
    let denominator = out_of(dataset);
    let mut notes = Vec::new();

    for column in dataset.drifting() {
        // The read type is named once, so notes about one column name it alike.
        let mut types: Vec<&DataType> = vec![&column.dtype];
        types.extend(column.conflicting_types.iter());
        let names = distinct_names(&types);
        let (chosen, others) = names.split_first().expect("the chosen type is first");

        // Missing, conflicting and narrower are each true on its own, each said on its own.
        notes.extend(absence_note(
            column,
            readable,
            &denominator,
            dataset.column_ranges.get(&column.name),
            &scope,
        ));
        notes.extend(conflict_note(column, dataset, chosen, others, &scope));
        notes.extend(widening_note(column, chosen, &scope));
    }

    for column in &dataset.read_as_text {
        notes.push(text_note(column, &scope));
    }

    notes.extend(empty_files_note(dataset, &scope));
    notes.extend(row_group_note(dataset, &scope));
    notes.extend(small_files_note(dataset, &scope));
    notes.extend(partition_layout_note(dataset));
    notes.extend(skipped_files_note(dataset));

    if !dataset.unreadable.is_empty() {
        notes.push(Note {
            summary: format!(
                "{} unreadable, left out",
                how_many(dataset, dataset.unreadable.len())
            ),
            scope: scope.clone(),
            read_as_text: None,
            passed_over: None,
        });
    }

    notes
}

/// Rows a filter or sort leaves out because their files do not hold the named column
/// in its read type: the one note about the view. Phrased about the files (a footer
/// fact), not as "the view is N rows shorter", which an earlier filter may have made
/// false; so it also appears where those rows were already gone.
pub fn left_out_note(
    column: &ColumnDrift,
    dataset: &DatasetSchema,
    rows: usize,
    filtered: bool,
    sorted: bool,
) -> Note {
    let what = match (filtered, sorted) {
        (true, true) => "filter and sort",
        (true, false) => "filter",
        // Called only for a column the view names, so it names it one way or the other.
        _ => "sort",
    };
    let there = if rows == 1 {
        "1 row".to_string()
    } else {
        format!("{} rows", group_chrome(rows))
    };
    Note {
        summary: format!(
            "{}: {there} in {} left out of the {what}",
            column.name,
            how_many(dataset, column.conflicting_files)
        ),
        scope: format!("in {}", dataset.origin),
        read_as_text: None,
        passed_over: None,
    }
}

/// Files holding no rows (an empty day's partition, a header-only file): not a fault,
/// but directories then overcount days. Counts without dividing. Says "file" even when
/// sampled (a footer records rows, it holds none); the scope line says what was read.
fn empty_files_note(dataset: &DatasetSchema, scope: &str) -> Option<Note> {
    let empty = dataset.empty_files;
    if empty == 0 {
        return None;
    }
    let (count, verb) = if empty == 1 {
        ("1 file".to_string(), "holds")
    } else {
        (format!("{} files", group_chrome(empty)), "hold")
    };
    Some(Note {
        summary: format!("{count} {verb} no rows"),
        scope: scope.to_string(),
        read_as_text: None,
        passed_over: None,
    })
}

/// Row groups big enough that a page reads much more than the page: a row group is
/// fetched whole, so over a network scrolling waits on it, and the user cannot fix it
/// here. Fires past one noticeable download, on the median group size over every
/// footer read (each group once, not row-weighted). States the group's size, not a
/// page's cost (pages skip binary columns, see `binary_stub_exprs`).
fn row_group_note(dataset: &DatasetSchema, scope: &str) -> Option<Note> {
    /// Sixty-four mebibytes, the size a page in that range costs to reach.
    const BIG: usize = 64 * 1024 * 1024;
    let median = dataset.median_row_group_bytes?;
    if median <= BIG {
        return None;
    }
    Some(Note {
        summary: format!(
            "median row group {}, each read whole",
            crate::numfmt::bytes(median as u64)
        ),
        scope: scope.to_string(),
        read_as_text: None,
        passed_over: None,
    })
}

/// A dataset of very many, very small files: says a footer was read for every one
/// before any row (footers are file suffixes, so finding never costs more than
/// reading). Both conditions must hold; the fix is upstream. The count is every listed
/// file, the median size over footers opened, which the sentence names.
fn small_files_note(dataset: &DatasetSchema, scope: &str) -> Option<Note> {
    /// Past this many files the footer pass is a job of its own. Above a year of
    /// hourly partitions, which is an ordinary shape and not a complaint.
    const MANY: usize = 10_000;
    /// Below this a file is small by any warehouse's standard, where the figure aimed
    /// at is hundreds of megabytes.
    const SMALL: usize = 1024 * 1024;
    let files = dataset.origin.total_files();
    let median = dataset.median_file_bytes?;
    // A zero median means sizes are unknown, not small.
    if median == 0 || files <= MANY || median >= SMALL {
        return None;
    }
    let read = dataset.files;
    // "opened for its footer" (an unparsable footer was still opened), joined by a
    // semicolon: only the count, not the size, causes one read per file.
    let footers = if read == files {
        "every footer".to_string()
    } else {
        format!("{} footers", group_chrome(read))
    };
    Some(Note {
        summary: format!(
            "{} files, median {}; {footers} read before any row",
            group_chrome(files),
            crate::numfmt::bytes(median as u64)
        ),
        scope: scope.to_string(),
        read_as_text: None,
        passed_over: None,
    })
}

/// Directories that do not all partition by the same keys. States the shape only:
/// the consequence (null column or failed open) depends on which path sorts first,
/// which a note cannot see; the user guide explains. Read from every file name, so it
/// has its own scope line and covers all of a too-large dataset.
fn partition_layout_note(dataset: &DatasetSchema) -> Option<Note> {
    /// Layouts named before the rest are counted rather than spelled. A note is one
    /// sentence, and a dataset with a hundred layouts would otherwise make it a page.
    const NAMED: usize = 2;
    if dataset.partition_layouts.len() < 2 {
        return None;
    }
    let (named, rest) = dataset
        .partition_layouts
        .split_at(dataset.partition_layouts.len().min(NAMED));
    let mut clauses: Vec<String> = named
        .iter()
        .map(|(keys, files)| format!("{} by {}", how_many_files(*files), keys.join("/")))
        .collect();
    let (dropped_ways, dropped_files) = dataset.partition_layouts_dropped;
    let ways = rest.len() + dropped_ways;
    let files: usize = rest.iter().map(|(_, files)| files).sum::<usize>() + dropped_files;
    if ways > 0 {
        clauses.push(format!(
            "{} by {} other {}",
            how_many_files(files),
            group_chrome(ways),
            if ways == 1 { "way" } else { "ways" }
        ));
    }
    Some(Note {
        summary: format!("mixed partition keys: {}", clauses.join(", ")),
        scope: format!(
            "in the names of {} files",
            group_chrome(dataset.listed_files)
        ),
        read_as_text: None,
        passed_over: None,
    })
}

/// `n files`, or `1 file`. Plain files, because the caller counted every one of them.
fn how_many_files(n: usize) -> String {
    format!(
        "{} {}",
        group_chrome(n),
        if n == 1 { "file" } else { "files" }
    )
}

/// A column read as text from every file on request, replacing its conflict note: a
/// filter or sort on it now compares text (`n > 5` keeps `"sixty"`, drops `"10"`).
fn text_note(column: &PlSmallStr, scope: &str) -> Note {
    Note {
        summary: format!("{column} read as text: filter and sort compare text"),
        scope: scope.to_string(),
        read_as_text: None,
        passed_over: None,
    }
}

/// Files beside the data that are not Parquet, so not in the table: only those someone
/// might have meant as data (not `_SUCCESS`, `.crc`, `_metadata`, which every job
/// leaves). Counts them beside what they accompany, as far as the listing saw.
fn skipped_files_note(dataset: &DatasetSchema) -> Option<Note> {
    note_about_skipped(dataset.skipped)
}

/// [`skipped_files_note`]'s note from the tally alone, so [`merged`] can rebuild it
/// with the open's files removed.
fn note_about_skipped(skipped: SkippedFiles) -> Option<Note> {
    let SkippedFiles {
        bookkeeping,
        not_parquet,
        empty,
    } = skipped;
    if not_parquet == 0 && empty == 0 {
        return None;
    }
    let files = |n: usize| {
        if n == 1 {
            "1 file".to_string()
        } else {
            format!("{} files", group_chrome(n))
        }
    };
    // An object with nothing in it is a write that stopped, and saying so is the point;
    // the rest is counted beside it so the total is the directory's, not a selection.
    let mut said = Vec::new();
    if empty > 0 {
        let what = if empty == 1 { "file" } else { "files" };
        said.push(format!("{} empty {what}", group_chrome(empty)));
    }
    if not_parquet > 0 {
        said.push(format!("{} not Parquet", files(not_parquet)));
    }
    if bookkeeping > 0 {
        let what = if bookkeeping == 1 { "file" } else { "files" };
        said.push(format!(
            "{} writer bookkeeping {what}",
            group_chrome(bookkeeping)
        ));
    }
    Some(Note {
        summary: format!("skipped: {}", said.join(", ")),
        scope: "in this directory's listing".to_string(),
        read_as_text: None,
        passed_over: None,
    })
}

/// The most names a note lists before it cuts the rest with an ellipsis.
const NAMES_SHOWN: usize = 3;

/// `names`, the first few of them, joined, and an ellipsis for the rest.
pub fn some_names<S: AsRef<str>>(names: &[S]) -> String {
    let mut said: Vec<&str> = names.iter().take(NAMES_SHOWN).map(AsRef::as_ref).collect();
    let ellipsis = crate::glyphs::get().ellipsis;
    if names.len() > NAMES_SHOWN {
        said.push(ellipsis);
    }
    said.join(", ")
}

/// The files a read of several passed over because they hold no header: empty, blank,
/// or nothing but NUL padding.
pub fn no_header(files: &[&std::path::Path]) -> Option<Note> {
    if files.is_empty() {
        return None;
    }
    let names: Vec<String> = files
        .iter()
        .map(|f| {
            f.file_name().map_or_else(
                || f.display().to_string(),
                |n| n.to_string_lossy().into_owned(),
            )
        })
        .collect();
    let what = if files.len() == 1 { "file" } else { "files" };
    Some(Note {
        summary: format!(
            "{} {what} with no header skipped: {}",
            files.len(),
            some_names(&names)
        ),
        scope: "empty, blank, or only NUL padding".to_string(),
        read_as_text: None,
        passed_over: None,
    })
}

/// What the open has to say before any footer: which data files this read passed
/// over, and whether a lake table is read as its plain files. Choices, not defects:
/// datui never refuses a read asked for, and says what it did instead.
pub fn from_the_open(
    left_out: &[(crate::FileFormat, usize)],
    lake: Option<&str>,
    files_differ: crate::formats::schema_union::Disagreement,
    names_look_like_data: bool,
) -> Vec<Note> {
    let mut notes = Vec::new();
    // What stacking the files required, as found. Footerless formats have no per-column
    // tally (Parquet gets the exact version from its footers); the scope is a spread of
    // three files.
    let scope = || "in a spread of this directory's files".to_string();
    if files_differ.columns {
        notes.push(Note {
            summary: "columns differ across files: a missing column reads null".to_string(),
            scope: scope(),
            read_as_text: None,
            passed_over: None,
        });
    }
    if files_differ.headerless {
        notes.push(Note {
            summary: concat!(
                "no header row? first row read as names: ",
                "H on Schema, or --no-header, reads it as data"
            )
            .to_string(),
            scope: scope(),
            read_as_text: None,
            passed_over: None,
        });
    }
    // The same shape in one file: every column name a number, which a header almost
    // never is and a first row of data often is.
    if names_look_like_data && !files_differ.headerless {
        notes.push(Note {
            summary: "column names look like data: H on Schema reads them as a row".to_string(),
            scope: "from the column names".to_string(),
            read_as_text: None,
            passed_over: None,
        });
    }
    if files_differ.types {
        notes.push(Note {
            summary: "a column's type differs across files: read as the wider type".to_string(),
            scope: scope(),
            read_as_text: None,
            passed_over: None,
        });
    }
    if let Some(format) = lake {
        notes.push(Note {
            // The strongest sentence: here a number on screen is not about the table (deleted rows,
            // old versions and compaction leftovers all counted).
            summary: format!(
                "{format} table's files, not the table: deleted rows and old versions counted"
            ),
            scope: format!("in this {format} table's directory"),
            read_as_text: None,
            passed_over: None,
        });
    }
    if !left_out.is_empty() {
        let said: Vec<String> = left_out
            .iter()
            .map(|(format, n)| format!("{n} {}", format.name()))
            .collect();
        notes.push(Note {
            summary: format!(
                "mixed formats, read as the commonest: {} not read",
                said.join(", ")
            ),
            scope: "in this directory's listing".to_string(),
            read_as_text: None,
            // Carried so [`merged`] can take these files back out of the footer
            // pass's tally, which walks the same directory and counts them again.
            passed_over: Some(left_out.iter().map(|(_, n)| n).sum()),
        });
    }
    notes
}

/// The `cache-*.arrow` files `map()` wrote beside a Hugging Face cache's splits, which
/// the read left out: their columns are the mapping's, not the split's.
pub fn map_caches(count: usize) -> Option<Note> {
    let files = if count == 1 { "file" } else { "files" };
    (count > 0).then(|| Note {
        summary: format!("{count} cache {files} written by map() not read"),
        scope: "in this directory's listing".to_string(),
        read_as_text: None,
        passed_over: None,
    })
}

/// Every note the panel shows (the open's, the footers', the view's), with the one fact
/// the first two both report said once: a mixed directory's skipped files appear in the
/// open's note and the footer walk's. The open's sentence is kept; the walk's is rebuilt
/// without those files, keeping skips only it saw.
pub fn merged(
    open: &[Note],
    dataset: &[Note],
    view: &[Note],
    schema: Option<&DatasetSchema>,
) -> Vec<Note> {
    // The walk's note as is and without the open's files; matched as a whole note, and
    // left alone if it is not one this module wrote.
    let rebuilt: Option<(Note, Option<Note>)> = match (
        open.iter().find_map(|n| n.passed_over),
        schema.map(|s| s.skipped),
    ) {
        (Some(covered), Some(skipped)) => note_about_skipped(skipped).map(|full| {
            let remaining = SkippedFiles {
                // Saturating: the walk counts more than the open, but "0 files are not Parquet" must
                // stay unwritable.
                not_parquet: skipped.not_parquet.saturating_sub(covered),
                ..skipped
            };
            (full, note_about_skipped(remaining))
        }),
        _ => None,
    };
    let mut out: Vec<Note> = open.to_vec();
    for note in dataset {
        match &rebuilt {
            Some((full, reduced)) if note == full => out.extend(reduced.clone()),
            _ => out.push(note.clone()),
        }
    }
    out.extend(view.iter().cloned());
    out
}

/// A column that some files were written without. The one note that states a ratio,
/// because "some" is only meaningful against a total.
fn absence_note(
    column: &ColumnDrift,
    readable: usize,
    denominator: &str,
    range: Option<&ColumnRange>,
    scope: &str,
) -> Option<Note> {
    if column.present_in == 0 || column.present_in >= readable {
        return None;
    }
    // Where, as well as how many. A count says a column is unusual; a partition says
    // where to look, and for a field a feed started sending it says when.
    let where_it_is = match range {
        Some(ColumnRange::Only(partition)) => format!(", only {partition}"),
        Some(ColumnRange::NoneBefore(partition)) => format!(", none before {partition}"),
        None => String::new(),
    };
    Some(Note {
        summary: format!(
            "{} is in {} of {}{}; absent from the rest, not null",
            column.name,
            group_chrome(column.present_in),
            denominator,
            where_it_is
        ),
        scope: scope.to_string(),
        read_as_text: None,
        passed_over: None,
    })
}

/// A column whose files disagree on type beyond widening; counts files without
/// dividing.
fn conflict_note(
    column: &ColumnDrift,
    dataset: &DatasetSchema,
    chosen: &str,
    others: &[String],
    scope: &str,
) -> Option<Note> {
    if column.conflicting_files == 0 {
        return None;
    }
    Some(Note {
        summary: format!(
            "{} is {} in {}; read as {} and not read there",
            column.name,
            others.join(" or "),
            how_many(dataset, column.conflicting_files),
            chosen
        ),
        scope: scope.to_string(),
        // The offer only where it works (a list column cannot be text); `lenient_scan` asks the
        // same question, so they agree.
        read_as_text: column.can_read_as_text().then(|| column.name.clone()),
        passed_over: None,
    })
}

/// A column stored in several types that the scan reads into one. Says only that: not
/// "width" (units, grown structs and untyped files land here too), nor "without loss"
/// (huge integers as floats, ms datetimes past 2262 as ns).
fn widening_note(column: &ColumnDrift, chosen: &str, scope: &str) -> Option<Note> {
    if !column.widened {
        return None;
    }
    Some(Note {
        summary: format!(
            "{} is stored as more than one type; read as {chosen}",
            column.name
        ),
        scope: scope.to_string(),
        read_as_text: None,
        passed_over: None,
    })
}

#[cfg(test)]
mod tests;
