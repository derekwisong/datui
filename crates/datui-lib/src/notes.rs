//! What datui noticed about a dataset while doing what it was already doing.
//!
//! A note never costs a request or a scan of its own: every one here is read off the
//! footers the schema and the row count already needed. Notes are never alarming — no
//! pop-up, no error styling — and every one says what it is based on, so "in 1 of 3
//! files" is never mistaken for a claim about files datui has not looked at.
//!
//! A note is one sentence and the line it rests on. Review after review found false
//! or empty statements here, and every one of them was in prose that went beyond the
//! sentence: a denominator over the wrong population, an explanatory line that
//! contradicted the note above it, a type named by a word two types share. What is
//! left is what can be checked: one claim per note, and one function deciding the only
//! ratio any of them states.

use crate::numfmt::group_chrome;
use crate::schema_union::{ColumnDrift, ColumnRange, DatasetSchema, SchemaOrigin, SkippedFiles};
use polars::prelude::{DataType, PlSmallStr};

/// One thing datui noticed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
    /// The one line shown in the panel.
    pub summary: String,
    /// What the note is based on, so its reach is never overstated.
    pub scope: String,
    /// The column datui can offer to read as text, when this note is about one and
    /// reading it that way would work. `None` for every other note.
    ///
    /// The name rather than the prose, because the panel has to act on it: matching a
    /// column out of a sentence is the kind of thing this module exists to avoid.
    pub read_as_text: Option<PlSmallStr>,
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

/// What the dataset's one ratio is out of: the files datui read and could parse.
///
/// This is the only denominator in the module. Where datui read every file and all of
/// them parsed they are simply "files"; where it sampled, or where a footer would not
/// parse, saying "files" would claim more than it looked at.
fn out_of(dataset: &DatasetSchema) -> String {
    let readable = dataset.files.saturating_sub(dataset.unreadable.len());
    match (sampled(dataset), dataset.unreadable.is_empty()) {
        (false, true) => how_many(dataset, readable),
        (false, false) => format!("the {} that could be read", how_many(dataset, readable)),
        (true, true) => format!("the {} read", how_many(dataset, readable)),
        (true, false) => format!("the {} that could be read", how_many(dataset, readable)),
    }
}

/// Names for a set of types that tell them apart.
///
/// Several types print alike at any given level of detail: every `Struct` is
/// `struct[1]` however its fields differ, and every `Enum` is `Enum([...])` however its
/// categories do. A note reading "s is struct[1] in 1 file; read as struct[1]" says
/// nothing, so the names escalate until they tell the types apart. Polars' `Display`
/// collapses too — a struct is `struct[1]` — so the full form is its `Debug`.
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
    // Polars prints every `Enum` as `Enum([...])` and every global `Categorical` as
    // `Categorical`, whatever their categories, so even the full form can collide. Numbering them says less than naming them, but "the first
    // and the second" is at least two things rather than one said twice. Only the ones
    // that actually collide are numbered; a name that was already unique keeps it.
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
        // The type the column is read as is named once, so two notes about the same
        // column cannot name it two different ways: whatever it takes to tell the
        // conflicting types apart is what the widening note calls it too.
        let mut types: Vec<&DataType> = vec![&column.dtype];
        types.extend(column.conflicting_types.iter());
        let names = distinct_names(&types);
        let (chosen, others) = names.split_first().expect("the chosen type is first");

        // A column can be missing from some files, stored differently in others, and
        // stored in a narrower type among the rest. Each is true on its own, so each
        // is said on its own: nothing here suppresses anything else.
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
                "{} could not be read and {} left out; the rows are not shown",
                how_many(dataset, dataset.unreadable.len()),
                if dataset.unreadable.len() == 1 {
                    "was"
                } else {
                    "were"
                }
            ),
            scope: scope.clone(),
            read_as_text: None,
        });
    }

    notes
}

/// Rows a filter or sort leaves out, because the column it names is not read from the
/// files those rows came from.
///
/// The only note here about the view rather than the dataset, and the only one datui
/// writes in answer to something the user just did.
///
/// It is phrased about the files, not about the view, and that is the whole care of
/// it. "2 rows are left out" reads as a claim that the view is two rows shorter, which
/// is not true when a filter had already dropped one of them; how many rows are in the
/// files that hold the column in another type is a fact of the footers, true whatever
/// else the view is doing. Both counts here are of that kind.
///
/// The cost of saying it that way is that the note also appears where those rows had
/// already gone — a filter that excluded them, then a sort on the column. It is still
/// true there, and the alternative is a count of what the view actually lost, which
/// cannot be had without collecting the frame twice.
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
    let (there, verb) = if rows == 1 {
        ("1 row".to_string(), "is")
    } else {
        (format!("{} rows", group_chrome(rows)), "are")
    };
    Note {
        summary: format!(
            "{} is not read from {}, so the {there} there {verb} left out of the {what}",
            column.name,
            how_many(dataset, column.conflicting_files)
        ),
        scope: format!("in {}", dataset.origin),
        read_as_text: None,
    }
}

/// Files that hold no rows at all.
///
/// A partition written for a day nothing happened, or a job that produced a header and
/// no data. Worth saying because the dataset then has fewer days of data than it has
/// folders, and a reader counting folders would get the wrong answer — but it is not a
/// fault, and a pipeline that writes a file per day will have some.
///
/// Counts without dividing: how many of the files datui read hold nothing is a fact
/// about those files, and the scope line says which files those were.
///
/// The one counting note that says "file" even where the schema came from a sample,
/// rather than the "footer" [`how_many`] would give it. A footer does not hold rows —
/// it records how many the file holds — so the substitution that keeps the other notes
/// honest makes this one a category slip. It costs nothing here: there is no ratio to
/// overstate, and the scope line already says only a sample was read, so "1 file holds
/// no rows · in 20,000 of 500,000 footers (sample)" claims nothing about the other
/// 480,000.
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
    })
}

/// Row groups big enough that reading a page means reading a lot more than the page.
///
/// A row group is what a reader fetches: a hundred rows anywhere inside one costs the
/// whole of it. Over a network that is the difference between a page arriving and a
/// page arriving after sixty-four megabytes do, and there is nothing the user can do
/// about it from here — which is exactly why it is worth saying rather than leaving
/// them to wonder why scrolling is slow.
///
/// The threshold is the size at which one row group is a noticeable download on an
/// ordinary connection; below it, nobody needs telling. States the middle size rather
/// than the largest: one row group of a gigabyte among thousands of small ones is a
/// different dataset from one where every row group is a gigabyte, and only the second
/// is worth a note.
///
/// Says "the middle row group is", not "row groups are, apiece" — the middle of
/// `[1 MiB, 100 MiB, 100 MiB]` is 100 MiB and one of those row groups is not.
///
/// And says the size, not what a page costs to fetch. They are not the same number:
/// a page projects away binary columns (see `binary_stub_exprs`), so their chunks are
/// never downloaded, while this size counts every chunk in the group. The figure is
/// the row group's; what follows it is why a row group's size is the one that matters.
///
/// The middle is over every row group of every footer read, each counting once. Not
/// weighted by rows, though a page is likelier to land in a group that holds more of
/// them: a dataset of one file of ten thousand small groups beside a hundred files of
/// one huge group each is called small by this and would be called large by that.
/// Counting groups is the statistic that matches the sentence — how big a row group
/// is, of the row groups there are.
fn row_group_note(dataset: &DatasetSchema, scope: &str) -> Option<Note> {
    /// Sixty-four mebibytes, the size a page in that range costs to reach.
    const BIG: usize = 64 * 1024 * 1024;
    let median = dataset.median_row_group_bytes?;
    if median <= BIG {
        return None;
    }
    Some(Note {
        summary: format!(
            "the middle row group is {}, and rows are read a row group at a time",
            crate::widgets::info::format_bytes(median as u64)
        ),
        scope: scope.to_string(),
        read_as_text: None,
    })
}

/// A dataset of very many files, each holding very little.
///
/// Says what happened, not what it cost. The first draft of this note said finding the
/// files costs more than reading them, and review measured it: a thousand two hundred
/// files took nine milliseconds to find and eight hundred to read. It cannot be true
/// over a network either — a footer read is a *suffix* of the file, so it can never
/// move more bytes than reading the file does. What is true, and is the thing the user
/// waited for, is that a footer was read for every one of these files before a single
/// row was.
///
/// Both halves have to hold. Small files on their own are a normal day's partitions,
/// and a few large ones cost nothing to open. It is very many *and* very small that
/// makes the opening a job of its own — and one nothing here can fix, since the remedy
/// is upstream in whatever writes them.
///
/// The count is every file the listing found; the middle size is over the footers
/// datui opened, which the sentence names. The two are different populations where the
/// dataset was too large to open every footer, and saying both numbers is what keeps
/// the middle from reading as a fact about all of them.
fn small_files_note(dataset: &DatasetSchema, scope: &str) -> Option<Note> {
    /// Past this many files the footer pass is a job of its own. Above a year of
    /// hourly partitions, which is an ordinary shape and not a complaint.
    const MANY: usize = 10_000;
    /// Below this a file is small by any warehouse's standard, where the figure aimed
    /// at is hundreds of megabytes.
    const SMALL: usize = 1024 * 1024;
    let files = dataset.origin.total_files();
    let median = dataset.median_file_bytes?;
    // A file of no bytes is not a Parquet file, so a middle of zero means datui does
    // not know the sizes rather than that they are small — and "the middle one is 0 B"
    // would be a claim about a dataset that cannot exist.
    if median == 0 || files <= MANY || median >= SMALL {
        return None;
    }
    let read = dataset.files;
    // "opened for its footer", not "a footer was read": a footer that would not parse
    // was still opened for, and the note beside this one says three of them were.
    //
    // And a semicolon, not "so": the footer pass is one per file whatever the files
    // hold, so only the count leads to it. Joining the two with "so" would make the
    // size look like half the reason.
    let footers = if read == files {
        "each was opened for its footer".to_string()
    } else {
        format!("{} were opened for their footers", group_chrome(read))
    };
    Some(Note {
        summary: format!(
            "there are {} files and the middle one is {}; {footers} before a row was",
            group_chrome(files),
            crate::widgets::info::format_bytes(median as u64)
        ),
        scope: scope.to_string(),
        read_as_text: None,
    })
}

/// Folders that do not all partition by the same keys.
///
/// Says the shape and stops there. What it *costs* is not something this note can see.
/// The scan reads its partition columns off one branch of the tree, and which branch
/// that is comes back from the filesystem in whatever order it likes. Usually every
/// file under the other key then fails and the dataset does not open at all.
///
/// But not always, and what decides it is not the branch — it is which file name sorts
/// first. The scan hands Polars its paths sorted, and Polars takes the hive schema from
/// the first of them: put one unpartitioned file at the root and whether it sorts above
/// `date=` decides whether the dataset opens with the column null or fails to open. A
/// file called `data.parquet` does; one called `loose.parquet` does not. Nothing a note
/// can see, and about as good a reason as there could be for a note not to say what
/// something costs.
///
/// Two rounds were spent on sentences that picked one of those and stated it as the
/// consequence. The user guide has room to set them out; a note has one sentence, and
/// the sentence true of every such dataset is the shape itself.
///
/// Read off the names of every file, which is the one thing the listing knows that
/// reading a file cannot tell you — so it has a scope line of its own, and on a dataset
/// too large to open every footer this note still saw all of it.
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
        summary: format!(
            "the folders do not all partition by the same keys: {}",
            clauses.join(", ")
        ),
        scope: format!(
            "in the names of {} files",
            group_chrome(dataset.listed_files)
        ),
        read_as_text: None,
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

/// A column being read as text from every file, because it was asked for that way.
///
/// Stands in for the conflict note it replaced, and says the one thing that changes
/// about the column beyond what is now visible in it: a filter or sort on it compares
/// text. `n > 5` written for a number keeps `"sixty"` and drops `"10"`, and a view
/// that quietly did that with nothing on screen to say so would be a view the user
/// reads wrongly.
fn text_note(column: &PlSmallStr, scope: &str) -> Note {
    Note {
        summary: format!("{column} is read as text, so a filter or sort on it compares text"),
        scope: scope.to_string(),
        read_as_text: None,
    }
}

/// Files in the folder that are not Parquet, and so are not in the table.
///
/// Only the ones somebody might have meant as data. A writer leaves `_SUCCESS`, `.crc`
/// and `_metadata` beside what it wrote, and saying so on every folder a job produced
/// would put an accent on the Info key for the most ordinary thing a folder can
/// contain — the same reason the small-files note waits for a threshold rather than
/// firing on every folder with more than one file in it. Where the note does fire it
/// counts them beside what it is about, so the numbers are the folder's rather than a
/// selection from it — as far as the listing saw, which is not the same as all of it:
/// a subtree it could not read, or one below the depth it stops at, is in neither.
fn skipped_files_note(dataset: &DatasetSchema) -> Option<Note> {
    let SkippedFiles {
        bookkeeping,
        not_parquet,
        empty,
    } = dataset.skipped;
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
    // the rest is counted beside it so the total is the folder's, not a selection.
    let mut said = Vec::new();
    if empty > 0 {
        said.push(format!(
            "{} {} empty and {} not read",
            files(empty),
            if empty == 1 { "is" } else { "are" },
            if empty == 1 { "was" } else { "were" }
        ));
    }
    if not_parquet > 0 {
        said.push(format!(
            "{} {} not Parquet",
            files(not_parquet),
            if not_parquet == 1 { "is" } else { "are" }
        ));
    }
    if bookkeeping > 0 {
        said.push(format!("{} a writer left behind", files(bookkeeping)));
    }
    Some(Note {
        summary: format!("in the folder, {}", said.join(", ")),
        scope: "in this folder's listing".to_string(),
        read_as_text: None,
    })
}

/// What the open itself has to say, before a footer has been read.
///
/// Two facts, both decided by the route that opened the folder rather than by anything
/// in the data: which of the folder's data files this read passed over, and whether the
/// folder is a lake table being read as its plain files. Neither is a defect in the
/// data — they are what datui chose to do, and #275's rule is that datui never refuses
/// a read the user asked for and always says what it did instead.
pub fn from_the_open(
    left_out: &[(crate::FileFormat, usize)],
    lake: Option<&str>,
    files_differ: crate::schema_union::Disagreement,
) -> Vec<Note> {
    let mut notes = Vec::new();
    // What the read had to do to stack them, in the words of what it actually found.
    // These formats carry no footer, so there is no per-column tally behind either
    // sentence; a Parquet dataset gets the exact version instead — which columns, in
    // how many files, and where — from footers it had to read anyway.
    //
    // The scope says a spread, because that is what was looked at: three files, the
    // ends and the middle, whatever the folder's size.
    let scope = || "in a spread of this folder's files".to_string();
    if files_differ.columns {
        notes.push(Note {
            summary: concat!(
                "the folder's files do not all have the same columns; the table has ",
                "every column any of them has, and a row from a file without one ",
                "reads null"
            )
            .to_string(),
            scope: scope(),
            read_as_text: None,
        });
    }
    if files_differ.types {
        notes.push(Note {
            summary: concat!(
                "a column is held in more than one type across the files, so it is ",
                "read as the wider of them — a number stored as text in one file ",
                "makes the whole column text, and it sorts and filters as text"
            )
            .to_string(),
            scope: scope(),
            read_as_text: None,
        });
    }
    if let Some(format) = lake {
        notes.push(Note {
            // The strongest sentence the panel has, because it is the one place a
            // number on screen is not a number about the table. A delete leaves its
            // rows on disk, an update leaves the version it replaced, and compaction
            // leaves both sides — all of them counted here.
            summary: format!(
                "these are the files under a {format} table, not the table:                  deleted rows and old versions are counted"
            ),
            scope: format!("in this {format} table's folder"),
            read_as_text: None,
        });
    }
    if !left_out.is_empty() {
        let said: Vec<String> = left_out
            .iter()
            .map(|(format, n)| format!("{n} {}", format.name()))
            .collect();
        notes.push(Note {
            summary: format!(
                "the folder holds more than one format and was read as the commonest;                  {} not read",
                said.join(", ")
            ),
            scope: "in this folder's listing".to_string(),
            read_as_text: None,
        });
    }
    notes
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
    })
}

/// A column whose files disagree on its type beyond what widening can settle.
///
/// Counts without dividing: how many files hold it in a type that lost is a fact about
/// those files, and needs no total to be true.
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
        // The offer, and only where it would work: a column one file holds as a list
        // cannot be shown as text at all, and an offer that did nothing would be worse
        // than none. `lenient_scan` asks the same question again, so the two cannot
        // disagree about which columns are on offer.
        read_as_text: column.can_read_as_text().then(|| column.name.clone()),
    })
}

/// A column the files store in more than one type, where the scan reads them all into
/// one.
///
/// Says only that: not "width", since a datetime unit, a struct that gained a field and
/// a file that never typed the column all land here, and not "without loss", since a
/// very large integer read as a float, or a millisecond datetime read as nanoseconds
/// past the year 2262, is not exact.
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_union::{FileSchema, union_file_schemas};
    use polars::prelude::{DataType, Field, Schema, TimeUnit, TimeZone};
    use std::sync::Arc;

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

    /// One dataset shape, and the notes it should produce.
    #[derive(Default)]
    struct Shape {
        what: &'static str,
        files: Vec<Option<FileSchema>>,
        /// `Some(total)` when the footers stand in for a larger dataset.
        sampled: Option<usize>,
        /// The file names, where the shape is about how they are laid out rather than
        /// about what is in them. Empty for a shape that has nothing to say about it.
        paths: Vec<&'static str>,
        expected: Vec<&'static str>,
    }

    fn dataset_for(shape: &Shape) -> DatasetSchema {
        let origin = match shape.sampled {
            Some(total) => SchemaOrigin::FooterSample {
                read: shape.files.len(),
                total,
            },
            None => SchemaOrigin::AllFooters(shape.files.len()),
        };
        let dataset = union_file_schemas(&shape.files, origin);
        if shape.paths.is_empty() {
            return dataset;
        }
        let paths: Vec<String> = shape.paths.iter().map(|p| p.to_string()).collect();
        dataset.with_partition_layouts("d", &paths)
    }

    fn notes_for(shape: &Shape) -> Vec<String> {
        from_dataset(&dataset_for(shape))
            .into_iter()
            .map(|n| n.summary)
            .collect()
    }

    /// Every shape of disagreement, and every line each one produces.
    ///
    /// Round after round of review found a wrong denominator in this module, each in a case
    /// the previous fix had not considered. The wording layer now states one ratio and
    /// counts everything else without dividing, and this lists the shapes end to end so
    /// the next wrong one shows up as a diff. Deliberately a transcript rather than a
    /// set of properties: the failure mode was plausible-looking prose, and a property
    /// would have to encode the same reasoning that kept going wrong.
    #[test]
    fn every_shape_of_disagreement_reads_the_way_it_should() {
        let i32 = DataType::Int32;
        let i64 = DataType::Int64;
        let str = DataType::String;
        let with_n = |a: DataType, b: DataType| {
            vec![
                file(&[("id", i64.clone()), ("n", a)], 50),
                file(&[("id", i64.clone()), ("n", b)], 50),
            ]
        };

        let cases = vec![
            // --- folders that disagree about what they are partitioned by ---
            Shape {
                what: "one folder under another key",
                files: vec![
                    file(&[("id", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64)], 1),
                ],
                paths: vec!["d/date=1/a.parquet", "d/dt=2/b.parquet"],
                expected: vec![
                    "the folders do not all partition by the same keys: 1 file by \
                     date, 1 file by dt",
                ],
                ..Shape::default()
            },
            Shape {
                what: "folders that agree",
                files: vec![
                    file(&[("id", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64)], 1),
                ],
                paths: vec!["d/date=1/a.parquet", "d/date=2/b.parquet"],
                expected: vec![],
                ..Shape::default()
            },
            // --- where a column that is not in every file sits ---
            Shape {
                what: "a column only one partition has",
                files: vec![
                    file(&[("id", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("oops", DataType::String)], 1),
                    file(&[("id", DataType::Int64)], 1),
                ],
                paths: vec![
                    "d/date=2024-03-01/a.parquet",
                    "d/date=2024-03-02/b.parquet",
                    "d/date=2024-03-03/c.parquet",
                ],
                expected: vec![
                    "oops is in 1 of 3 files, only date=2024-03-02; absent from \
                     the rest, not null",
                ],
                ..Shape::default()
            },
            Shape {
                what: "a column the feed started sending",
                files: vec![
                    file(&[("id", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                ],
                paths: vec![
                    "d/date=2010-07-17/a.parquet",
                    "d/date=2010-07-18/b.parquet",
                    "d/date=2010-07-19/c.parquet",
                ],
                expected: vec![
                    "fee is in 2 of 3 files, none before date=2010-07-18; absent from \
                     the rest, not null",
                ],
                ..Shape::default()
            },
            Shape {
                what: "a column in some files but no pattern to where",
                files: vec![
                    file(&[("id", DataType::Int64), ("odd", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("odd", DataType::Int64)], 1),
                ],
                paths: vec![
                    "d/date=1/a.parquet",
                    "d/date=2/b.parquet",
                    "d/date=3/c.parquet",
                ],
                expected: vec!["odd is in 2 of 3 files; absent from the rest, not null"],
                ..Shape::default()
            },
            Shape {
                what: "a column starting where the listing and the reader disagree",
                files: vec![
                    file(&[("id", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                ],
                // Sorted bytewise, `part=10` comes second. Saying "from part=10 on"
                // would tell a reader that parts 2 and 3 are without it, and they are
                // not: there is no honest way to say where this one starts.
                paths: vec![
                    "d/part=1/a.parquet",
                    "d/part=10/b.parquet",
                    "d/part=2/c.parquet",
                    "d/part=3/d.parquet",
                ],
                expected: vec!["fee is in 3 of 4 files; absent from the rest, not null"],
                ..Shape::default()
            },
            Shape {
                what: "a column starting at a month spelled two ways",
                files: vec![
                    file(&[("id", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                ],
                // `m=03` is March and so is `m=3` — a backfill beside a job. March
                // lacks the column, so "none before m=3" says it starts at a month
                // whose folder does not have it.
                paths: vec!["d/m=03/a.parquet", "d/m=3/b.parquet", "d/m=4/c.parquet"],
                expected: vec!["fee is in 2 of 3 files; absent from the rest, not null"],
                ..Shape::default()
            },
            Shape {
                what: "a column of a dataset whose folders name their keys in two orders",
                files: vec![
                    file(&[("id", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                ],
                // Hive matches partition columns by name, so these two folders are one
                // partition written both ways round. Saying the column begins at the
                // second would be saying it begins where the first is.
                paths: vec!["d/m=03/y=2024/a.parquet", "d/y=2024/m=03/b.parquet"],
                // And nothing else says so: the layouts note compares which keys a
                // folder uses, not the order it writes them in, so these two agree.
                // This note staying quiet is the only thing between a reader and a
                // sentence about a place that is written down twice.
                expected: vec!["fee is in 1 of 2 files; absent from the rest, not null"],
                ..Shape::default()
            },
            Shape {
                what: "a column two files of one partition have",
                files: vec![
                    file(&[("id", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                ],
                // The ordinary shape: a partition holds more than one file. Counting
                // its partition twice would make it look like two, and the note that
                // names it would quietly stop appearing.
                paths: vec![
                    "d/date=2024-03-01/a.parquet",
                    "d/date=2024-03-02/b.parquet",
                    "d/date=2024-03-02/c.parquet",
                ],
                expected: vec![
                    "fee is in 2 of 3 files, only date=2024-03-02; absent from the \
                     rest, not null",
                ],
                ..Shape::default()
            },
            Shape {
                what: "a column starting halfway through a partition",
                files: vec![
                    file(&[("id", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                ],
                // `date=2024-01-02` holds one file with `fee` and one without, so it is
                // not a date the column begins at.
                paths: vec![
                    "d/date=2024-01-01/a.parquet",
                    "d/date=2024-01-02/b.parquet",
                    "d/date=2024-01-02/c.parquet",
                    "d/date=2024-01-03/d.parquet",
                ],
                expected: vec!["fee is in 2 of 4 files; absent from the rest, not null"],
                ..Shape::default()
            },
            Shape {
                what: "a column whose partition holds the file without it",
                files: vec![
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64)], 1),
                ],
                // The file without it is at `y=2024/m=03`, which is under `y=2024`.
                // "only y=2024" would send a reader to a folder that holds it.
                paths: vec!["d/y=2024/a.parquet", "d/y=2024/m=03/b.parquet"],
                expected: vec![
                    "fee is in 1 of 2 files; absent from the rest, not null",
                    // Ragged depth is a disagreement in its own right, and says so.
                    "the folders do not all partition by the same keys: 1 file by m/y, \
                     1 file by y",
                ],
                ..Shape::default()
            },
            Shape {
                what: "a column of a dataset partitioned more than one level deep",
                files: vec![
                    file(&[("id", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                ],
                paths: vec!["d/y=2024/m=02/a.parquet", "d/y=2024/m=03/b.parquet"],
                expected: vec![
                    "fee is in 1 of 2 files, only y=2024/m=03; absent from the rest, \
                     not null",
                ],
                ..Shape::default()
            },
            Shape {
                what: "a column in a file that sits under no partition at all",
                files: vec![
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64)], 1),
                ],
                // One file at the root beside the partition folders. Half the files
                // that have `fee` are not under `date=2024-01-02`, so there is no
                // "only" to be had — and saying it anyway sends a reader to the
                // wrong folder, which is worse than the count on its own.
                paths: vec![
                    "d/aaa.parquet",
                    "d/date=2024-01-02/b.parquet",
                    "d/date=2024-01-03/c.parquet",
                ],
                expected: vec!["fee is in 2 of 3 files; absent from the rest, not null"],
                ..Shape::default()
            },
            Shape {
                what: "a column whose files are all in the one partition anyway",
                files: vec![
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64)], 1),
                ],
                // Every file is under `date=2024-03-02`, the one without it included.
                // "only" earns its place by contrast with the files that are not,
                // and there are none: the phrase would say nothing while sounding as
                // though the missing file were somewhere else.
                paths: vec![
                    "d/date=2024-03-02/a.parquet",
                    "d/date=2024-03-02/b.parquet",
                    "d/date=2024-03-02/c.parquet",
                ],
                expected: vec!["fee is in 2 of 3 files; absent from the rest, not null"],
                ..Shape::default()
            },
            Shape {
                what: "a column of a dataset with a footer that would not parse",
                files: vec![
                    file(&[("id", DataType::Int64)], 1),
                    None,
                    file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
                ],
                // A file whose footer would not read counts as missing nothing, which
                // makes it look like a file that has the column. Anchoring on it would
                // put a partition datui never opened into a sentence whose own scope
                // says it read two files.
                paths: vec!["d/x=1/a.parquet", "d/x=2/b.parquet", "d/x=3/c.parquet"],
                expected: vec![
                    "fee is in 1 of the 2 files that could be read; absent from the \
                     rest, not null",
                    "1 file could not be read and was left out; the rows are not shown",
                ],
                ..Shape::default()
            },
            Shape {
                what: "a column of a dataset whose footers were sampled",
                files: vec![
                    file(&[("id", DataType::Int64)], 1),
                    file(&[("id", DataType::Int64), ("oops", DataType::String)], 1),
                ],
                // A file whose footer was not read looks like a file missing nothing,
                // so where a column begins cannot be told from the two that were.
                sampled: Some(6541),
                paths: vec!["d/date=2024-03-01/a.parquet", "d/date=2024-03-02/b.parquet"],
                expected: vec![
                    "oops is in 1 of the 2 footers read; absent from the rest, not null",
                ],
            },
            // --- files that hold nothing at all ---
            Shape {
                what: "one empty file",
                files: vec![
                    file(&[("id", DataType::Int64)], 0),
                    file(&[("id", DataType::Int64)], 5),
                ],
                sampled: None,
                paths: Vec::new(),
                expected: vec!["1 file holds no rows"],
            },
            Shape {
                what: "several empty files",
                files: vec![
                    file(&[("id", DataType::Int64)], 0),
                    file(&[("id", DataType::Int64)], 0),
                    file(&[("id", DataType::Int64)], 5),
                ],
                sampled: None,
                paths: Vec::new(),
                expected: vec!["2 files hold no rows"],
            },
            Shape {
                what: "an empty file among sampled footers",
                files: vec![
                    file(&[("id", DataType::Int64)], 0),
                    file(&[("id", DataType::Int64)], 5),
                ],
                sampled: Some(900),
                paths: Vec::new(),
                // "file", not "footer": a footer does not hold rows. The scope line
                // is what says datui looked at two of nine hundred, and the note
                // claims nothing about the other 898.
                expected: vec!["1 file holds no rows"],
            },
            Shape {
                what: "an empty file and a column only the other has",
                files: vec![
                    file(&[("id", DataType::Int64)], 0),
                    file(&[("id", DataType::Int64), ("x", DataType::String)], 5),
                ],
                sampled: None,
                paths: Vec::new(),
                expected: vec![
                    "x is in 1 of 2 files; absent from the rest, not null",
                    "1 file holds no rows",
                ],
            },
            // --- a column that only some files have: the one note with a ratio ---
            Shape {
                what: "absent",
                files: vec![
                    file(&[("id", i64.clone())], 1),
                    file(&[("id", i64.clone()), ("x", str.clone())], 1),
                ],
                sampled: None,
                paths: Vec::new(),
                expected: vec!["x is in 1 of 2 files; absent from the rest, not null"],
            },
            Shape {
                what: "absent, sampled",
                files: vec![
                    file(&[("id", i64.clone())], 1),
                    file(&[("id", i64.clone()), ("x", str.clone())], 1),
                ],
                sampled: Some(200_000),
                paths: Vec::new(),
                expected: vec!["x is in 1 of the 2 footers read; absent from the rest, not null"],
            },
            Shape {
                what: "absent, with an unreadable footer",
                files: vec![
                    file(&[("id", i64.clone())], 1),
                    None,
                    file(&[("id", i64.clone()), ("x", str.clone())], 1),
                ],
                sampled: None,
                paths: Vec::new(),
                expected: vec![
                    "x is in 1 of the 2 files that could be read; absent from the rest, not null",
                    "1 file could not be read and was left out; the rows are not shown",
                ],
            },
            Shape {
                what: "absent, sampled, with an unreadable footer",
                files: vec![
                    file(&[("id", i64.clone())], 1),
                    None,
                    file(&[("id", i64.clone()), ("x", str.clone())], 1),
                ],
                sampled: Some(200_000),
                paths: Vec::new(),
                expected: vec![
                    "x is in 1 of the 2 footers that could be read; absent from the rest, not null",
                    "1 footer could not be read and was left out; the rows are not shown",
                ],
            },
            // --- a type the files disagree about: counted, never divided ---
            Shape {
                what: "conflicting",
                files: vec![
                    file(&[("n", str.clone())], 10),
                    file(&[("n", i64.clone())], 90),
                ],
                sampled: None,
                paths: Vec::new(),
                expected: vec!["n is str in 1 file; read as i64 and not read there"],
            },
            Shape {
                what: "conflicting, sampled",
                files: vec![
                    file(&[("n", str.clone())], 10),
                    file(&[("n", i64.clone())], 90),
                ],
                sampled: Some(200_000),
                paths: Vec::new(),
                expected: vec!["n is str in 1 footer; read as i64 and not read there"],
            },
            Shape {
                what: "conflicting, with an unreadable footer",
                files: vec![
                    file(&[("n", str.clone())], 10),
                    None,
                    file(&[("n", i64.clone())], 90),
                ],
                sampled: None,
                paths: Vec::new(),
                expected: vec![
                    "n is str in 1 file; read as i64 and not read there",
                    "1 file could not be read and was left out; the rows are not shown",
                ],
            },
            Shape {
                what: "conflicting, sampled, with an unreadable footer",
                files: vec![
                    file(&[("n", str.clone())], 10),
                    None,
                    file(&[("n", i64.clone())], 90),
                ],
                sampled: Some(200_000),
                paths: Vec::new(),
                expected: vec![
                    "n is str in 1 footer; read as i64 and not read there",
                    "1 footer could not be read and was left out; the rows are not shown",
                ],
            },
            // --- widening, which settles without loss ---
            Shape {
                what: "widened",
                files: with_n(i32.clone(), i64.clone()),
                sampled: None,
                paths: Vec::new(),
                expected: vec!["n is stored as more than one type; read as i64"],
            },
            // --- and the combinations, each saying all of what is true ---
            Shape {
                what: "absent and conflicting",
                files: vec![
                    file(&[("n", str.clone())], 10),
                    file(&[("n", i64.clone())], 90),
                    file(&[("id", i64.clone())], 5),
                ],
                sampled: None,
                paths: Vec::new(),
                // Schema order: the newest file's columns lead, so `id` comes first.
                expected: vec![
                    "id is in 1 of 3 files; absent from the rest, not null",
                    "n is in 2 of 3 files; absent from the rest, not null",
                    "n is str in 1 file; read as i64 and not read there",
                ],
            },
            Shape {
                what: "absent and widened",
                files: vec![
                    file(&[("id", i64.clone())], 1),
                    file(&[("id", i64.clone()), ("n", i32.clone())], 50),
                    file(&[("id", i64.clone()), ("n", i64.clone())], 50),
                ],
                sampled: None,
                paths: Vec::new(),
                expected: vec![
                    "n is in 2 of 3 files; absent from the rest, not null",
                    "n is stored as more than one type; read as i64",
                ],
            },
            Shape {
                what: "conflicting and widened",
                files: vec![
                    file(&[("n", i32.clone())], 50),
                    file(&[("n", i64.clone())], 50),
                    file(&[("n", str.clone())], 5),
                ],
                sampled: None,
                paths: Vec::new(),
                expected: vec![
                    "n is str in 1 file; read as i64 and not read there",
                    "n is stored as more than one type; read as i64",
                ],
            },
            Shape {
                what: "a chosen type no file stores",
                files: vec![
                    file(&[("n", i32.clone())], 50),
                    file(&[("n", DataType::Float32)], 50),
                    file(&[("n", str.clone())], 5),
                ],
                sampled: None,
                paths: Vec::new(),
                expected: vec![
                    "n is str in 1 file; read as f64 and not read there",
                    "n is stored as more than one type; read as f64",
                ],
            },
            Shape {
                what: "two types the table spells the same way",
                files: vec![
                    file(
                        &[("t", DataType::Datetime(TimeUnit::Nanoseconds, None))],
                        10,
                    ),
                    file(
                        &[(
                            "t",
                            DataType::Datetime(TimeUnit::Nanoseconds, Some(TimeZone::UTC)),
                        )],
                        90,
                    ),
                ],
                sampled: None,
                paths: Vec::new(),
                // `datetime in 1 file; read as datetime` would say nothing, so the
                // note spells the types out where the short words collide.
                expected: vec![
                    "t is datetime[ns] in 1 file; read as datetime[ns, UTC] and not read there",
                ],
            },
            Shape {
                what: "two conflicting types the table spells the same way",
                files: vec![
                    file(
                        &[("t", DataType::Datetime(TimeUnit::Nanoseconds, None))],
                        10,
                    ),
                    file(
                        &[(
                            "t",
                            DataType::Datetime(TimeUnit::Nanoseconds, Some(TimeZone::UTC)),
                        )],
                        10,
                    ),
                    file(&[("t", str.clone())], 90),
                ],
                sampled: None,
                paths: Vec::new(),
                // The two that lost share a word as much as either shares one with the
                // winner, so all three are spelled out.
                expected: vec![
                    "t is datetime[ns] or datetime[ns, UTC] in 2 files; read as str and not read there",
                ],
            },
            Shape {
                what: "two structs, which Display also spells the same way",
                files: vec![
                    file(
                        &[(
                            "s",
                            DataType::Struct(vec![Field::new("a".into(), i64.clone())]),
                        )],
                        90,
                    ),
                    file(
                        &[(
                            "s",
                            DataType::Struct(vec![Field::new("a".into(), str.clone())]),
                        )],
                        10,
                    ),
                ],
                sampled: None,
                paths: Vec::new(),
                // `struct[1]` for both, so only the fields tell them apart.
                expected: vec![
                    "s is Struct({'a': String}) in 1 file; \
                     read as Struct({'a': Int64}) and not read there",
                ],
            },
            Shape {
                what: "a chosen type whose word another type shares",
                files: vec![
                    file(
                        &[("t", DataType::Datetime(TimeUnit::Milliseconds, None))],
                        50,
                    ),
                    file(
                        &[("t", DataType::Datetime(TimeUnit::Nanoseconds, None))],
                        50,
                    ),
                    file(&[("t", str.clone())], 5),
                ],
                sampled: None,
                paths: Vec::new(),
                // Both notes name the winner the same way, and the way the Schema tab
                // does: "read as datetime" would drop the unit that is the point.
                expected: vec![
                    "t is str in 1 file; read as datetime[ns] and not read there",
                    "t is stored as more than one type; read as datetime[ns]",
                ],
            },
            Shape {
                what: "widened between two datetime units",
                files: vec![
                    file(
                        &[("t", DataType::Datetime(TimeUnit::Milliseconds, None))],
                        50,
                    ),
                    file(
                        &[("t", DataType::Datetime(TimeUnit::Nanoseconds, None))],
                        50,
                    ),
                ],
                sampled: None,
                paths: Vec::new(),
                // Which unit won is the whole content of the note, and `datetime`
                // alone would not carry it.
                expected: vec!["t is stored as more than one type; read as datetime[ns]"],
            },
            Shape {
                what: "a struct that both widens and conflicts",
                files: vec![
                    file(
                        &[(
                            "s",
                            DataType::Struct(vec![Field::new("a".into(), i32.clone())]),
                        )],
                        50,
                    ),
                    file(
                        &[(
                            "s",
                            DataType::Struct(vec![Field::new("a".into(), i64.clone())]),
                        )],
                        50,
                    ),
                    file(
                        &[(
                            "s",
                            DataType::Struct(vec![Field::new("a".into(), str.clone())]),
                        )],
                        5,
                    ),
                ],
                sampled: None,
                paths: Vec::new(),
                // Both notes name the winner the same way. Naming it separately let
                // one say `Struct({'a': Int64})` and the other `struct[1]`.
                expected: vec![
                    "s is Struct({'a': String}) in 1 file; \
                     read as Struct({'a': Int64}) and not read there",
                    "s is stored as more than one type; read as Struct({'a': Int64})",
                ],
            },
            Shape {
                what: "a decimal, whose precision the header word drops",
                files: vec![
                    file(&[("d", DataType::Decimal(38, 2))], 90),
                    file(&[("d", str.clone())], 10),
                ],
                sampled: None,
                paths: Vec::new(),
                expected: vec!["d is str in 1 file; read as decimal[38,2] and not read there"],
            },
            Shape {
                what: "absent, conflicting and widened",
                files: vec![
                    file(&[("id", i64.clone())], 1),
                    file(&[("id", i64.clone()), ("n", i32.clone())], 50),
                    file(&[("id", i64.clone()), ("n", i64.clone())], 50),
                    file(&[("id", i64.clone()), ("n", str.clone())], 5),
                ],
                sampled: None,
                paths: Vec::new(),
                expected: vec![
                    "n is in 3 of 4 files; absent from the rest, not null",
                    "n is str in 1 file; read as i64 and not read there",
                    "n is stored as more than one type; read as i64",
                ],
            },
        ];

        for shape in &cases {
            assert_eq!(notes_for(shape), shape.expected, "{}", shape.what);
        }

        // A column some file holds in another type always draws a note of its own.
        //
        // The table's view notes lean on this: `DataTableState::has_notes` answers
        // whether the Notes tab is on offer from the dataset's notes alone, which is
        // only sound if a note about what a sort leaves out can never be the only one
        // there. Every shape above that has a conflicting column is a case of it.
        for shape in &cases {
            let dataset = dataset_for(shape);
            let conflicting: Vec<&str> = dataset
                .columns
                .iter()
                .filter(|column| column.conflicting_files > 0)
                .map(|column| column.name.as_str())
                .collect();
            for name in conflicting {
                // The conflict note itself, not merely some note naming the column:
                // an absence or widening note about the same column would satisfy a
                // looser test while the one that matters had been deleted.
                assert!(
                    from_dataset(&dataset).iter().any(|note| {
                        note.summary.starts_with(&format!("{name} is "))
                            && note.summary.ends_with("and not read there")
                    }),
                    "{}: {name} conflicts, so it says so on its own account",
                    shape.what
                );
            }
        }
    }

    /// The last resort, when a type's own `Debug` does not tell it from another's.
    ///
    /// Polars prints every `Enum` as `Enum([...])` and every global `Categorical` as
    /// `Categorical`, whatever their categories, so two of either collide through the
    /// short word, through `Display` and through `Debug` alike. Those are awkward to
    /// build here, so this drives the same branch with names that collide outright.
    /// Numbering says less than naming would, but it is two things rather than one
    /// thing said twice.
    #[test]
    fn types_that_print_alike_all_the_way_down_are_numbered() {
        let names = distinct_names(&[&DataType::Int64, &DataType::Int64]);
        assert_eq!(names, ["Int64 #1", "Int64 #2"]);

        // A name that never collided keeps it.
        let mixed = distinct_names(&[&DataType::Int64, &DataType::Int64, &DataType::String]);
        // Once any pair collides every name comes from `Debug`, so `str` is `String`
        // here; only the colliding pair carries a number.
        assert_eq!(mixed, ["Int64 #1", "Int64 #2", "String"]);
    }

    #[test]
    fn a_uniform_dataset_has_nothing_to_say() {
        let shape = Shape {
            what: "uniform",
            files: vec![
                file(&[("id", DataType::Int64)], 1),
                file(&[("id", DataType::Int64)], 1),
            ],
            sampled: None,
            paths: Vec::new(),
            expected: vec![],
        };
        assert!(notes_for(&shape).is_empty());
    }

    /// The summary and the scope line sit one above the other, so a reader takes them
    /// together. Nothing in a summary may claim more than the scope allows.
    #[test]
    fn no_summary_claims_more_than_its_scope() {
        let files = [
            file(&[("id", DataType::Int64)], 1),
            None,
            file(&[("id", DataType::Int64), ("x", DataType::String)], 1),
        ];
        for (origin, expected_scope) in [
            (SchemaOrigin::AllFooters(3), "in all 3 footers"),
            (
                SchemaOrigin::FooterSample {
                    read: 3,
                    total: 200_000,
                },
                "in 3 of 200,000 footers (sample)",
            ),
        ] {
            let dataset = union_file_schemas(&files, origin);
            let notes = from_dataset(&dataset);
            assert_eq!(notes.len(), 2, "an absence note and an unreadable one");
            for note in notes {
                assert_eq!(note.scope, expected_scope);
                // The only ratio in the module is over what could be read, and it never
                // claims the whole dataset.
                assert!(
                    !note.summary.contains("of 3 files"),
                    "one of those three said nothing: {}",
                    note.summary
                );
            }
        }
    }

    /// Two notes about one column must each say which files they mean, rather than
    /// both saying "the others" about different sets.
    #[test]
    fn the_notes_about_one_column_do_not_talk_past_each_other() {
        let files = [
            file(&[("n", DataType::String)], 10),
            file(&[("n", DataType::Int64)], 90),
            file(&[("id", DataType::Int64)], 5),
        ];
        let dataset = union_file_schemas(&files, SchemaOrigin::AllFooters(3));
        let about_n: Vec<String> = from_dataset(&dataset)
            .into_iter()
            .map(|note| note.summary)
            .filter(|summary| summary.starts_with('n'))
            .collect();
        assert_eq!(
            about_n,
            [
                "n is in 2 of 3 files; absent from the rest, not null",
                "n is str in 1 file; read as i64 and not read there",
            ]
        );
    }
}
