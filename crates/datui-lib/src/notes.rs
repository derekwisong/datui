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
use crate::schema_union::{ColumnDrift, DatasetSchema, SchemaOrigin};
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
        notes.extend(absence_note(column, readable, &denominator, &scope));
        notes.extend(conflict_note(column, dataset, chosen, others, &scope));
        notes.extend(widening_note(column, chosen, &scope));
    }

    for column in &dataset.read_as_text {
        notes.push(text_note(column, &scope));
    }

    notes.extend(empty_files_note(dataset, &scope));
    notes.extend(row_group_note(dataset, &scope));

    if !dataset.unreadable.is_empty() {
        notes.push(Note {
            summary: format!(
                "{} could not be read and {} left out of the schema",
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
fn row_group_note(dataset: &DatasetSchema, scope: &str) -> Option<Note> {
    /// Sixty-four mebibytes, the size a page in that range costs to reach.
    const BIG: usize = 64 * 1024 * 1024;
    let median = dataset.median_row_group_bytes?;
    if median <= BIG {
        return None;
    }
    Some(Note {
        summary: format!(
            "row groups are {} apiece, and a page anywhere inside one reads all of it",
            crate::widgets::info::format_bytes(median as u64)
        ),
        scope: scope.to_string(),
        read_as_text: None,
    })
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

/// A column that some files were written without. The one note that states a ratio,
/// because "some" is only meaningful against a total.
fn absence_note(
    column: &ColumnDrift,
    readable: usize,
    denominator: &str,
    scope: &str,
) -> Option<Note> {
    if column.present_in == 0 || column.present_in >= readable {
        return None;
    }
    Some(Note {
        summary: format!(
            "{} is in {} of {}; absent from the rest, not null",
            column.name,
            group_chrome(column.present_in),
            denominator
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
            row_group_bytes: Vec::new(),
        })
    }

    /// One dataset shape, and the notes it should produce.
    struct Shape {
        what: &'static str,
        files: Vec<Option<FileSchema>>,
        /// `Some(total)` when the footers stand in for a larger dataset.
        sampled: Option<usize>,
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
        union_file_schemas(&shape.files, origin)
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
            // --- files that hold nothing at all ---
            Shape {
                what: "one empty file",
                files: vec![
                    file(&[("id", DataType::Int64)], 0),
                    file(&[("id", DataType::Int64)], 5),
                ],
                sampled: None,
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
                expected: vec!["2 files hold no rows"],
            },
            Shape {
                what: "an empty file among sampled footers",
                files: vec![
                    file(&[("id", DataType::Int64)], 0),
                    file(&[("id", DataType::Int64)], 5),
                ],
                sampled: Some(900),
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
                expected: vec!["x is in 1 of 2 files; absent from the rest, not null"],
            },
            Shape {
                what: "absent, sampled",
                files: vec![
                    file(&[("id", i64.clone())], 1),
                    file(&[("id", i64.clone()), ("x", str.clone())], 1),
                ],
                sampled: Some(200_000),
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
                expected: vec![
                    "x is in 1 of the 2 files that could be read; absent from the rest, not null",
                    "1 file could not be read and was left out of the schema",
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
                expected: vec![
                    "x is in 1 of the 2 footers that could be read; absent from the rest, not null",
                    "1 footer could not be read and was left out of the schema",
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
                expected: vec!["n is str in 1 file; read as i64 and not read there"],
            },
            Shape {
                what: "conflicting, sampled",
                files: vec![
                    file(&[("n", str.clone())], 10),
                    file(&[("n", i64.clone())], 90),
                ],
                sampled: Some(200_000),
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
                expected: vec![
                    "n is str in 1 file; read as i64 and not read there",
                    "1 file could not be read and was left out of the schema",
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
                expected: vec![
                    "n is str in 1 footer; read as i64 and not read there",
                    "1 footer could not be read and was left out of the schema",
                ],
            },
            // --- widening, which settles without loss ---
            Shape {
                what: "widened",
                files: with_n(i32.clone(), i64.clone()),
                sampled: None,
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
