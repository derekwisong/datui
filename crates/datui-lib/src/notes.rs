//! What datui noticed about a dataset while doing what it was already doing.
//!
//! A note never costs a request or a scan of its own: every one here is read off the
//! footers the schema and the row count already needed. Notes are never alarming — no
//! pop-up, no error styling — and every one says what it is based on, so "only in 1 of
//! 3 files" is never mistaken for a claim about files datui has not looked at.

use crate::numfmt::group_chrome;
use crate::schema_union::{ColumnDrift, DatasetSchema, SchemaOrigin};
use crate::widgets::datatable::dtype_label;

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

/// How many files, spelled for a sentence: "1 file", "3 files".
fn files(n: usize) -> String {
    if n == 1 {
        "1 file".to_string()
    } else {
        format!("{} files", group_chrome(n))
    }
}

/// What a count is out of, in the words that are true of it.
///
/// A count is only ever over the footers datui read. Where that is every file and all
/// of them parsed, they are simply "files"; where it is a sample, or where some footer
/// could not be read, saying "files" would claim more than datui looked at.
fn out_of(dataset: &DatasetSchema) -> String {
    let readable = dataset.files.saturating_sub(dataset.unreadable.len());
    let sampled = matches!(dataset.origin, SchemaOrigin::FooterSample { .. });
    match (sampled, dataset.unreadable.is_empty()) {
        (false, true) => files(readable),
        (false, false) => format!("the {} that could be read", files(readable)),
        (true, true) => format!("the {} footers read", group_chrome(readable)),
        // Both: the denominator is neither the files nor the footers asked for, and
        // saying either would disagree with the scope line under it.
        (true, false) => format!("the {} footers that could be read", group_chrome(readable)),
    }
}

/// What a count *within one column* is out of: the files that have it at all. A file
/// written without the column is not one of the files that disagree about its type.
fn out_of_those_with(dataset: &DatasetSchema, present_in: usize) -> String {
    if matches!(dataset.origin, SchemaOrigin::FooterSample { .. }) {
        format!("the {} footers that have it", group_chrome(present_in))
    } else {
        format!("the {} that have it", files(present_in))
    }
}

/// What a count of *attempted* footers is out of. Unlike `out_of`, the unreadable ones
/// are the subject rather than excluded from the denominator.
fn out_of_all(dataset: &DatasetSchema) -> String {
    if matches!(dataset.origin, SchemaOrigin::FooterSample { .. }) {
        format!("the {} footers sampled", group_chrome(dataset.files))
    } else {
        files(dataset.files)
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
        let conflict = conflict_note(
            column,
            &out_of_those_with(dataset, column.present_in),
            &scope,
        );
        let absence = absence_note(column, readable, &denominator, &scope);
        let said = conflict.is_some() || absence.is_some();
        notes.extend(conflict);
        notes.extend(absence);
        if !said && column.widened {
            notes.push(Note {
                summary: format!(
                    "{} is stored in more than one width; read as {}",
                    column.name,
                    dtype_label(&column.dtype)
                ),
                scope: scope.clone(),
                detail: vec!["Widening it loses nothing.".to_string()],
            });
        }
    }

    if !dataset.unreadable.is_empty() {
        notes.push(Note {
            summary: format!(
                "{} of {} could not be read and {} left out",
                group_chrome(dataset.unreadable.len()),
                out_of_all(dataset),
                if dataset.unreadable.len() == 1 {
                    "was"
                } else {
                    "were"
                }
            ),
            scope: scope.clone(),
            detail: vec![
                "The rest of the dataset opened without them.".to_string(),
                "Their columns are not in the schema; what the scan makes of their rows \
                 is up to the reader."
                    .to_string(),
            ],
        });
    }

    notes
}

/// A column whose files disagree on its type beyond what widening can settle.
fn conflict_note(column: &ColumnDrift, denominator: &str, scope: &str) -> Option<Note> {
    if column.conflicting_files == 0 {
        return None;
    }
    let others: Vec<String> = column.conflicting_types.iter().map(dtype_label).collect();
    Some(Note {
        summary: format!(
            "{} is {} in {} of {}, read as {} from the rest",
            column.name,
            others.join(" or "),
            group_chrome(column.conflicting_files),
            denominator,
            dtype_label(&column.dtype)
        ),
        scope: scope.to_string(),
        detail: vec![
            format!(
                "{} is the type most of its rows have.",
                dtype_label(&column.dtype)
            ),
            "The column is not read from the other files, so its cells there are a \
             conflict rather than a null."
                .to_string(),
        ],
    })
}

/// A column that some files were written without.
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
        detail: vec!["Rows from the other files show the column as absent, not null.".to_string()],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_union::{FileSchema, SchemaOrigin, union_file_schemas};
    use polars::prelude::{DataType, Schema};
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

    fn notes_of(files: &[Option<FileSchema>]) -> Vec<Note> {
        let dataset = union_file_schemas(files, SchemaOrigin::AllFooters(files.len()));
        from_dataset(&dataset)
    }

    #[test]
    fn a_uniform_dataset_has_nothing_to_say() {
        let files = [
            file(&[("id", DataType::Int64)], 1),
            file(&[("id", DataType::Int64)], 1),
        ];
        assert!(notes_of(&files).is_empty());
    }

    #[test]
    fn a_column_only_some_files_have_is_noted_with_its_reach() {
        let files = [
            file(&[("id", DataType::Int64)], 1),
            file(&[("id", DataType::Int64), ("oops", DataType::String)], 1),
            file(&[("id", DataType::Int64)], 1),
        ];
        let notes = notes_of(&files);
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].summary, "oops is in 1 of 3 files");
        assert_eq!(notes[0].scope, "in all 3 footers");
    }

    #[test]
    fn a_type_conflict_names_both_types_and_says_which_won() {
        let files = [
            file(&[("price", DataType::String)], 10),
            file(&[("price", DataType::Int64)], 90),
        ];
        let notes = notes_of(&files);
        assert_eq!(notes.len(), 1);
        assert_eq!(
            notes[0].summary,
            "price is str in 1 of the 2 files that have it, read as i64 from the rest"
        );
    }

    #[test]
    fn widening_is_noted_as_losing_nothing() {
        let files = [
            file(&[("n", DataType::Int32)], 1),
            file(&[("n", DataType::Int64)], 1),
        ];
        let notes = notes_of(&files);
        assert_eq!(notes.len(), 1);
        assert!(
            notes[0]
                .summary
                .starts_with("n is stored in more than one width")
        );
    }

    #[test]
    fn an_unreadable_footer_is_noted_and_the_rest_still_opens() {
        let files = [
            file(&[("id", DataType::Int64)], 1),
            None,
            file(&[("id", DataType::Int64)], 1),
        ];
        let notes = notes_of(&files);
        assert_eq!(notes.len(), 1);
        assert_eq!(
            notes[0].summary,
            "1 of 3 files could not be read and was left out"
        );
    }

    #[test]
    fn a_sampled_schema_says_so_in_every_note_it_makes() {
        let files = [
            file(&[("id", DataType::Int64)], 1),
            file(&[("id", DataType::Int64), ("oops", DataType::String)], 1),
        ];
        let dataset = crate::schema_union::union_file_schemas(
            &files,
            SchemaOrigin::FooterSample {
                read: 2,
                total: 200_000,
            },
        );
        let notes = from_dataset(&dataset);
        assert_eq!(notes.len(), 1);
        assert_eq!(
            notes[0].summary, "oops is in 1 of the 2 footers read",
            "a sample counts footers, not the files it did not look at"
        );
        assert_eq!(notes[0].scope, "in 2 of 200,000 footers (sample)");
    }

    /// A column can be missing from one file and stored differently in another. The
    /// conflict is only among the files that have it, and both facts are worth saying.
    #[test]
    fn a_column_both_missing_and_conflicting_says_each_over_the_right_total() {
        let files = [
            file(&[("price", DataType::String)], 10),
            file(&[("price", DataType::Int64)], 90),
            file(&[("id", DataType::Int64)], 5),
        ];
        let notes = notes_of(&files);
        let conflict = notes
            .iter()
            .find(|n| n.summary.contains("read as"))
            .expect("the type conflict is noted");
        assert_eq!(
            conflict.summary,
            "price is str in 1 of the 2 files that have it, read as i64 from the rest",
            "two files have `price`, not three"
        );
        let absence = notes
            .iter()
            .find(|n| n.summary.starts_with("price is in"))
            .expect("and so is the file that has no price at all");
        assert_eq!(
            absence.summary, "price is in 2 of 3 files",
            "the absence is over every file, the conflict over the two that have it"
        );
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
        let notes = notes_of(&files);
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

    /// A sample that also hit an unreadable footer is neither "files" nor "the footers
    /// read": both would disagree with the scope line under them.
    #[test]
    fn a_sample_with_an_unreadable_footer_still_agrees_with_its_scope() {
        let files = [
            file(&[("id", DataType::Int64)], 1),
            None,
            file(&[("id", DataType::Int64), ("oops", DataType::String)], 1),
        ];
        let dataset = union_file_schemas(
            &files,
            SchemaOrigin::FooterSample {
                read: 3,
                total: 200_000,
            },
        );
        let notes = from_dataset(&dataset);
        let absence = notes
            .iter()
            .find(|n| n.summary.starts_with("oops"))
            .expect("the column is noted");
        assert_eq!(
            absence.summary, "oops is in 1 of the 2 footers that could be read",
            "neither `2 files` nor `2 footers read` is true here"
        );
        let unreadable = notes
            .iter()
            .find(|n| n.summary.contains("could not be read"))
            .expect("and so is the footer that failed");
        assert_eq!(
            unreadable.summary,
            "1 of the 3 footers sampled could not be read and was left out"
        );
    }

    /// The count and the scope line sit next to each other, so they must not state
    /// different totals.
    #[test]
    fn a_count_never_contradicts_the_scope_beneath_it() {
        let files = [
            file(&[("id", DataType::Int64)], 1),
            None,
            file(&[("id", DataType::Int64), ("oops", DataType::String)], 1),
        ];
        let notes = notes_of(&files);
        let absence = notes
            .iter()
            .find(|n| n.summary.starts_with("oops"))
            .expect("the column is still noted");
        assert_eq!(
            absence.summary, "oops is in 1 of the 2 files that could be read",
            "not `of 2 files` under a scope line that says three footers"
        );
        assert_eq!(absence.scope, "in all 3 footers");
    }
}
