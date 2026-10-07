use super::*;

use crate::schema_union::{FileFooter, union_file_schemas};
use polars::prelude::{DataType, Field, Schema, TimeUnit, TimeZone};
use std::sync::Arc;

/// rustfmt joins a `\`-continued literal back onto one line with its indentation
/// inside, which put eighteen spaces into the middle of two of these sentences.
#[test]
fn the_notes_from_an_open_have_no_holes_in_them() {
    let notes = from_the_open(
        &[(crate::FileFormat::Json, 1)],
        Some("Delta"),
        crate::schema_union::Disagreement {
            columns: true,
            types: true,
            headerless: true,
        },
        false,
    );
    assert!(notes.len() >= 3, "{notes:?}");
    for note in &notes {
        assert!(!note.summary.contains("  "), "{:?}", note.summary);
    }
}

/// A dataset with nothing but a skip tally, as the footer walk hands one over.
fn walked(skipped: SkippedFiles) -> DatasetSchema {
    union_file_schemas(&[], SchemaOrigin::AllFooters(0)).with_skipped(skipped)
}

fn agree() -> crate::schema_union::Disagreement {
    crate::schema_union::Disagreement {
        columns: false,
        types: false,
        headerless: false,
    }
}

/// The open and the footer walk both count what a mixed directory's read passed
/// over, and on screen that was the same fact twice, one wording above the other.
/// The open's sentence names the formats and says why, so it is the one kept.
#[test]
fn a_mixed_directory_is_not_reported_twice() {
    let open = from_the_open(&[(crate::FileFormat::Csv, 1)], None, agree(), false);
    let dataset = walked(SkippedFiles {
        bookkeeping: 0,
        not_parquet: 1,
        empty: 0,
    });
    let said: Vec<String> = merged(&open, &from_dataset(&dataset), &[], Some(&dataset))
        .into_iter()
        .map(|n| n.summary)
        .collect();
    assert_eq!(
        said,
        ["mixed formats, read as the commonest: 1 csv not read"],
        "one fact, said once, in the open's words"
    );
}

/// What only the walk saw stays counted: a name no reader claims, an empty
/// object and a writer's bookkeeping are not among the formats the open named,
/// so taking the open's files out must not take these with them. The view's
/// notes still follow, untouched.
#[test]
fn what_only_the_walk_saw_stays_counted() {
    let open = from_the_open(&[(crate::FileFormat::Csv, 1)], None, agree(), false);
    let dataset = walked(SkippedFiles {
        bookkeeping: 2,
        not_parquet: 2,
        empty: 1,
    });
    let view = [text_note(&PlSmallStr::from("n"), "in 3 files")];
    let said: Vec<String> = merged(&open, &from_dataset(&dataset), &view, Some(&dataset))
        .into_iter()
        .map(|n| n.summary)
        .collect();
    assert_eq!(
        said,
        [
            "mixed formats, read as the commonest: 1 csv not read".to_string(),
            "skipped: 1 empty file, 1 file not Parquet, 2 writer bookkeeping files".to_string(),
            "n read as text: filter and sort compare text".to_string(),
        ],
        "the walk's own findings and the view's notes survive the merge"
    );
}

/// A tally the open never spoke to is left exactly as the walk wrote it: a hive
/// dataset's strays live below the top level, where the open's one-level look
/// never reaches, and its note is the only thing that reports them.
#[test]
fn a_walk_only_tally_is_left_alone() {
    let open = from_the_open(&[], Some("Delta"), agree(), false);
    let dataset = walked(SkippedFiles {
        bookkeeping: 0,
        not_parquet: 1,
        empty: 0,
    });
    let said: Vec<String> = merged(&open, &from_dataset(&dataset), &[], Some(&dataset))
        .into_iter()
        .map(|n| n.summary)
        .collect();
    assert_eq!(said.len(), 2, "{said:?}");
    assert!(
        said[1] == "skipped: 1 file not Parquet",
        "different facts do not merge: {said:?}"
    );
}

fn file(columns: &[(&str, DataType)], rows: usize) -> Option<FileFooter> {
    let mut schema = Schema::with_capacity(columns.len());
    for (name, dtype) in columns {
        schema.with_column((*name).into(), dtype.clone());
    }
    Some(FileFooter {
        schema: Arc::new(schema),
        row_group_rows: vec![rows],
        file_bytes: 0,
        row_group_bytes: Vec::new(),
        column_bytes: Vec::new(),
    })
}

/// One dataset shape, and the notes it should produce.
#[derive(Default)]
struct Shape {
    what: &'static str,
    files: Vec<Option<FileFooter>>,
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
        // --- directories that disagree about what they are partitioned by ---
        Shape {
            what: "one directory under another key",
            files: vec![
                file(&[("id", DataType::Int64)], 1),
                file(&[("id", DataType::Int64)], 1),
            ],
            paths: vec!["d/date=1/a.parquet", "d/dt=2/b.parquet"],
            expected: vec!["mixed partition keys: 1 file by date, 1 file by dt"],
            ..Shape::default()
        },
        Shape {
            what: "directories that agree",
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
            // whose directory does not have it.
            paths: vec!["d/m=03/a.parquet", "d/m=3/b.parquet", "d/m=4/c.parquet"],
            expected: vec!["fee is in 2 of 3 files; absent from the rest, not null"],
            ..Shape::default()
        },
        Shape {
            what: "a column of a dataset whose directories name their keys in two orders",
            files: vec![
                file(&[("id", DataType::Int64)], 1),
                file(&[("id", DataType::Int64), ("fee", DataType::Int64)], 1),
            ],
            // Hive matches partition columns by name, so these two directories are
            // one partition written both ways round. Saying the column begins at the
            // second would be saying it begins where the first is.
            paths: vec!["d/m=03/y=2024/a.parquet", "d/y=2024/m=03/b.parquet"],
            // And nothing else says so: the layouts note compares which keys a
            // directory uses, not the order it writes them in, so these two agree.
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
            // "only y=2024" would send a reader to a directory that holds it.
            paths: vec!["d/y=2024/a.parquet", "d/y=2024/m=03/b.parquet"],
            expected: vec![
                "fee is in 1 of 2 files; absent from the rest, not null",
                // Ragged depth is a disagreement in its own right, and says so.
                "mixed partition keys: 1 file by m/y, 1 file by y",
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
            // One file at the root beside the partition directories. Half the files
            // that have `fee` are not under `date=2024-01-02`, so there is no
            // "only" to be had — and saying it anyway sends a reader to the
            // wrong directory, which is worse than the count on its own.
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
                "1 file unreadable, left out",
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
            expected: vec!["oops is in 1 of the 2 footers read; absent from the rest, not null"],
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
                "1 file unreadable, left out",
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
                "1 footer unreadable, left out",
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
                "1 file unreadable, left out",
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
                "1 footer unreadable, left out",
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
