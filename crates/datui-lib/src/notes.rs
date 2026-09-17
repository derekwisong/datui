//! What datui noticed about a dataset while doing what it was already doing.
//!
//! A note never costs a request or a scan of its own: every one here is read off the
//! footers the schema and the row count already needed. Notes are never alarming — no
//! pop-up, no error styling — and every one says what it is based on, so "in 1 of 3
//! files" is never mistaken for a claim about files datui has not looked at.
//!
//! Only one note states a ratio, and one function decides what that ratio is over.
//! Four rounds of review found a wrong denominator here, each in a case the last fix
//! had not considered; the others now count without dividing, which is a claim that
//! cannot be wrong.

use crate::numfmt::group_chrome;
use crate::schema_union::{ColumnDrift, DatasetSchema, SchemaOrigin};
use crate::widgets::datatable::dtype_label;
use polars::prelude::DataType;

/// One thing datui noticed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
    /// The one line shown in the panel.
    pub summary: String,
    /// What the note is based on, so its reach is never overstated.
    pub scope: String,
    /// The particulars, shown when the note is opened.
    pub detail: Vec<String>,
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

/// What the footers said, as notes. Empty when every file agrees, which is the common
/// case and the one where there is nothing to say.
pub fn from_dataset(dataset: &DatasetSchema) -> Vec<Note> {
    let scope = format!("in {}", dataset.origin);
    let readable = dataset.files.saturating_sub(dataset.unreadable.len());
    let denominator = out_of(dataset);
    let mut notes = Vec::new();

    for column in dataset.drifting() {
        // A column can be missing from some files, stored differently in others, and
        // stored in two widths among the rest. Each is true on its own, so each is
        // said on its own: nothing here suppresses anything else.
        notes.extend(absence_note(column, readable, &denominator, &scope));
        notes.extend(conflict_note(column, dataset, &scope));
        notes.extend(widening_note(column, &scope));
    }

    if !dataset.unreadable.is_empty() {
        notes.push(Note {
            summary: format!(
                "{} could not be read and {} left out",
                how_many(dataset, dataset.unreadable.len()),
                if dataset.unreadable.len() == 1 {
                    "was"
                } else {
                    "were"
                }
            ),
            scope: scope.clone(),
            detail: vec![if dataset.unreadable.len() == 1 {
                "Its columns are not in the schema; what the scan makes of its rows \
                     is up to the reader."
                    .to_string()
            } else {
                "Their columns are not in the schema; what the scan makes of their \
                     rows is up to the reader."
                    .to_string()
            }],
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
    scope: &str,
) -> Option<Note> {
    if column.present_in == 0 || column.present_in >= readable {
        return None;
    }
    Some(Note {
        summary: format!(
            "{} is in {} of {}",
            column.name,
            group_chrome(column.present_in),
            denominator
        ),
        scope: scope.to_string(),
        detail: vec!["Files written without it show the column as absent, not null.".to_string()],
    })
}

/// A column whose files disagree on its type beyond what widening can settle.
///
/// Counts without dividing: how many files hold it in the type that lost is a fact
/// about those files, and needs no total to be true.
fn conflict_note(column: &ColumnDrift, dataset: &DatasetSchema, scope: &str) -> Option<Note> {
    if column.conflicting_files == 0 {
        return None;
    }
    // `dtype_label` is the table header's word for a type, and two types can share
    // one: `datetime` for either time zone, `struct` for any set of fields. A note
    // reading "t is datetime in 1 file; read as datetime" says nothing, so where the
    // short words collide the note spells the types out.
    let chosen = dtype_label(&column.dtype);
    let collides = column
        .conflicting_types
        .iter()
        .any(|other| dtype_label(other) == chosen);
    let name_of = |dtype: &DataType| {
        if collides {
            format!("{dtype}")
        } else {
            dtype_label(dtype)
        }
    };
    let others: Vec<String> = column.conflicting_types.iter().map(&name_of).collect();
    Some(Note {
        summary: format!(
            "{} is {} in {}; read as {}",
            column.name,
            others.join(" or "),
            how_many(dataset, column.conflicting_files),
            name_of(&column.dtype)
        ),
        scope: scope.to_string(),
        detail: vec![
            format!(
                "{} is the type that covers the most rows.",
                dtype_label(&column.dtype)
            ),
            "The column is not read from the files that disagree, so its cells there are \
             a conflict rather than a null."
                .to_string(),
        ],
    })
}

/// A column the files store in more than one type, where the scan can read them all
/// into one.
///
/// Says only that: not "width", since a datetime unit, a struct that gained a field and
/// a file that never typed the column all land here, and not "without loss", since a
/// very large integer read as a float, or a millisecond datetime read as nanoseconds
/// past the year 2262, is not exact.
fn widening_note(column: &ColumnDrift, scope: &str) -> Option<Note> {
    if !column.widened {
        return None;
    }
    Some(Note {
        summary: format!(
            "{} is stored as more than one type; read as {}",
            column.name,
            dtype_label(&column.dtype)
        ),
        scope: scope.to_string(),
        detail: vec![format!(
            "Every file's type is read as {}.",
            dtype_label(&column.dtype)
        )],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_union::{FileSchema, union_file_schemas};
    use polars::prelude::{DataType, Schema, TimeUnit, TimeZone};
    use std::sync::Arc;

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

    /// One dataset shape, and the notes it should produce.
    struct Shape {
        what: &'static str,
        files: Vec<Option<FileSchema>>,
        /// `Some(total)` when the footers stand in for a larger dataset.
        sampled: Option<usize>,
        expected: Vec<&'static str>,
    }

    fn notes_for(shape: &Shape) -> Vec<String> {
        let origin = match shape.sampled {
            Some(total) => SchemaOrigin::FooterSample {
                read: shape.files.len(),
                total,
            },
            None => SchemaOrigin::AllFooters(shape.files.len()),
        };
        let dataset = union_file_schemas(&shape.files, origin);
        from_dataset(&dataset)
            .into_iter()
            .map(|n| n.summary)
            .collect()
    }

    /// Every shape of disagreement, and every line each one produces.
    ///
    /// Four rounds of review found a wrong denominator in this module, each in a case
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
            // --- a column that only some files have: the one note with a ratio ---
            Shape {
                what: "absent",
                files: vec![
                    file(&[("id", i64.clone())], 1),
                    file(&[("id", i64.clone()), ("x", str.clone())], 1),
                ],
                sampled: None,
                expected: vec!["x is in 1 of 2 files"],
            },
            Shape {
                what: "absent, sampled",
                files: vec![
                    file(&[("id", i64.clone())], 1),
                    file(&[("id", i64.clone()), ("x", str.clone())], 1),
                ],
                sampled: Some(200_000),
                expected: vec!["x is in 1 of the 2 footers read"],
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
                    "x is in 1 of the 2 files that could be read",
                    "1 file could not be read and was left out",
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
                    "x is in 1 of the 2 footers that could be read",
                    "1 footer could not be read and was left out",
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
                expected: vec!["n is str in 1 file; read as i64"],
            },
            Shape {
                what: "conflicting, sampled",
                files: vec![
                    file(&[("n", str.clone())], 10),
                    file(&[("n", i64.clone())], 90),
                ],
                sampled: Some(200_000),
                expected: vec!["n is str in 1 footer; read as i64"],
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
                    "n is str in 1 file; read as i64",
                    "1 file could not be read and was left out",
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
                    "n is str in 1 footer; read as i64",
                    "1 footer could not be read and was left out",
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
                    "id is in 1 of 3 files",
                    "n is in 2 of 3 files",
                    "n is str in 1 file; read as i64",
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
                    "n is in 2 of 3 files",
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
                    "n is str in 1 file; read as i64",
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
                    "n is str in 1 file; read as f64",
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
                expected: vec!["t is datetime[ns] in 1 file; read as datetime[ns, UTC]"],
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
                    "n is in 3 of 4 files",
                    "n is str in 1 file; read as i64",
                    "n is stored as more than one type; read as i64",
                ],
            },
        ];

        for shape in &cases {
            assert_eq!(notes_for(shape), shape.expected, "{}", shape.what);
        }
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
            for note in from_dataset(&dataset) {
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

    /// The detail of the unreadable note claims only what the scan guarantees: the
    /// columns are gone, but the rows may well still be read.
    #[test]
    fn the_unreadable_note_claims_nothing_about_the_rows() {
        let files = [
            file(&[("id", DataType::Int64)], 1),
            None,
            file(&[("id", DataType::Int64)], 1),
        ];
        let dataset = union_file_schemas(&files, SchemaOrigin::AllFooters(3));
        let notes = from_dataset(&dataset);
        let detail = notes[0].detail.join(" ");
        assert!(
            detail.contains("columns are not in the schema"),
            "got: {detail}"
        );
        assert!(
            !detail.contains("rows are not counted"),
            "the scan is handed those files too, so this was never true: {detail}"
        );
    }

    /// Two notes about one column must not both say "the other files" and mean
    /// different sets.
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
            .filter(|note| note.summary.starts_with('n'))
            .flat_map(|note| note.detail)
            .collect();
        assert_eq!(about_n.len(), 3, "an absence note and a conflict note");
        assert!(
            !about_n.iter().any(|d| d.contains("the other files")),
            "each says which files it means: {about_n:?}"
        );
    }
}
