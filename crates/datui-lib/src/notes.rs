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
    /// How many files this note already counts as passed over by the read, when it is
    /// the one that says a mixed directory was read as its commonest format. `None`
    /// for every other note.
    ///
    /// The number rather than the prose, for the same reason as `read_as_text`:
    /// [`merged`] has to take these files back out of the footer pass's tally, and
    /// reading a count out of a sentence is the kind of thing this module exists to
    /// avoid.
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

/// Files that hold no rows at all.
///
/// A partition written for a day nothing happened, or a job that produced a header and no
/// data. Worth saying because the dataset then has fewer days of data than it has
/// directories, and a reader counting directories would get the wrong answer — but it is
/// not a fault, and a pipeline that writes a file per day will have some.
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
        passed_over: None,
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
            "median row group {}, each read whole",
            crate::numfmt::bytes(median as u64)
        ),
        scope: scope.to_string(),
        read_as_text: None,
        passed_over: None,
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

/// Directories that do not all partition by the same keys.
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

/// A column being read as text from every file, because it was asked for that way.
///
/// Stands in for the conflict note it replaced, and says the one thing that changes
/// about the column beyond what is now visible in it: a filter or sort on it compares
/// text. `n > 5` written for a number keeps `"sixty"` and drops `"10"`, and a view
/// that quietly did that with nothing on screen to say so would be a view the user
/// reads wrongly.
fn text_note(column: &PlSmallStr, scope: &str) -> Note {
    Note {
        summary: format!("{column} read as text: filter and sort compare text"),
        scope: scope.to_string(),
        read_as_text: None,
        passed_over: None,
    }
}

/// Files in the directory that are not Parquet, and so are not in the table.
///
/// Only the ones somebody might have meant as data. A writer leaves `_SUCCESS`, `.crc`
/// and `_metadata` beside what it wrote, and saying so on every directory a job produced
/// would put an accent on the Info key for the most ordinary thing a directory can
/// contain — the same reason the small-files note waits for a threshold rather than
/// firing on every directory with more than one file in it. Where the note does fire it
/// counts them beside what it is about, so the numbers are the directory's rather than a
/// selection from it — as far as the listing saw, which is not the same as all of it:
/// a subtree it could not read, or one below the depth it stops at, is in neither.
fn skipped_files_note(dataset: &DatasetSchema) -> Option<Note> {
    note_about_skipped(dataset.skipped)
}

/// The note [`skipped_files_note`] writes, from the tally alone.
///
/// Split out so [`merged`] can rebuild it with the files the open already reported
/// taken back out, without reading a count out of the note's own sentence.
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

/// What the open itself has to say, before a footer has been read.
///
/// Two facts, both decided by the route that opened the directory rather than by anything
/// in the data: which of the directory's data files this read passed over, and whether
/// the directory is a lake table being read as its plain files. Neither is a defect in
/// the data — they are what datui chose to do, and #275's rule is that datui never
/// refuses a read the user asked for and always says what it did instead.
pub fn from_the_open(
    left_out: &[(crate::FileFormat, usize)],
    lake: Option<&str>,
    files_differ: crate::schema_union::Disagreement,
    names_look_like_data: bool,
) -> Vec<Note> {
    let mut notes = Vec::new();
    // What the read had to do to stack them, in the words of what it actually found.
    // These formats carry no footer, so there is no per-column tally behind either
    // sentence; a Parquet dataset gets the exact version instead — which columns, in
    // how many files, and where — from footers it had to read anyway.
    //
    // The scope says a spread, because that is what was looked at: three files, the
    // ends and the middle, whatever the directory's size.
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
            // The strongest sentence the panel has, because it is the one place a
            // number on screen is not a number about the table. A delete leaves its
            // rows on disk, an update leaves the version it replaced, and compaction
            // leaves both sides — all of them counted here.
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

/// Every note the panel shows — what the open did, what the footers said, then what
/// the view leaves out — with the one fact the first two both report said once.
///
/// A directory of mixed formats read as its commonest is reported by the open ("read
/// as the commonest; 1 csv not read"), and then the footer pass walks the same
/// directory and counts the same files among what it passed ("in the directory, 1
/// file is not Parquet"). The open's sentence names the formats and says why they are
/// not in the table, so it is the one kept; the footer note is rebuilt with those
/// files taken back out, which keeps every skip only the walk can see — a stray in a
/// partition below the top level, a name no reader claims, an empty object, a
/// writer's bookkeeping. The two tallies never meet anywhere else: the open's is
/// settled before a footer is read, and the walk's arrives with the dataset, so the
/// caller that holds both halves hands them here.
pub fn merged(
    open: &[Note],
    dataset: &[Note],
    view: &[Note],
    schema: Option<&DatasetSchema>,
) -> Vec<Note> {
    // What the walk said as it stands, and with the open's files taken out. Matched
    // as a whole note rather than by its prose, and if the dataset does not hold
    // exactly that note — a shape this module did not write — nothing is touched.
    let rebuilt: Option<(Note, Option<Note>)> = match (
        open.iter().find_map(|n| n.passed_over),
        schema.map(|s| s.skipped),
    ) {
        (Some(covered), Some(skipped)) => note_about_skipped(skipped).map(|full| {
            let remaining = SkippedFiles {
                // Saturating: the open counts one level of recognized names, the
                // walk everything it saw, so the walk's tally is never smaller —
                // but a false "0 files are not Parquet" must stay unwritable.
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
        passed_over: None,
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
        passed_over: None,
    })
}

#[cfg(test)]
mod tests;
