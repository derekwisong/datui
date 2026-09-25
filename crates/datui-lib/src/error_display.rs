//! User-facing error message formatting.
//!
//! Uses typed error matching (PolarsError variants, io::ErrorKind) rather than
//! string parsing to produce actionable, implementation-agnostic messages.

use polars::prelude::PolarsError;
use std::io;
use std::path::Path;

/// Format a PolarsError as a user-facing message by matching on its variant.
pub fn user_message_from_polars(err: &PolarsError) -> String {
    // Polars' words first, then the tidying every one of them wants: its query plan
    // taken off the end, and the one shape worth rewriting said as a folder rather than
    // as two schemas printed in full. Done here, around the match, so no arm can be
    // added that forgets it.
    let said = polars_words(err);
    if is_union_schema_error(&said) {
        return union_schema_message(&said);
    }
    without_the_query_plan(&said)
}

fn polars_words(err: &PolarsError) -> String {
    use polars::prelude::PolarsError as PE;

    match err {
        PE::ColumnNotFound(msg) => format!(
            "Column not found: {}. Check spelling and that the column exists.",
            msg
        ),
        PE::Duplicate(msg) => format!(
            "Duplicate column in result: {}. Use aliases to rename columns, e.g. `select my_date: timestamp.date`",
            msg
        ),
        PE::IO { error, msg } => {
            user_message_from_io(error.as_ref(), msg.as_ref().map(|m| m.as_ref()))
        }
        PE::NoData(msg) => format!("No data: {}", msg),
        PE::SchemaMismatch(msg) => format!("Schema mismatch: {}", msg),
        PE::ShapeMismatch(msg) => format!("Row shape mismatch: {}", msg),
        PE::InvalidOperation(msg) => format!("Operation not allowed: {}", msg),
        PE::OutOfBounds(msg) => format!("Index or row out of bounds: {}", msg),
        PE::SchemaFieldNotFound(msg) => format!("Schema field not found: {}", msg),
        PE::StructFieldNotFound(msg) => format!("Struct field not found: {}", msg),
        PE::ComputeError(msg) => simplify_compute_message(msg),
        PE::AssertionError(msg) => format!("Assertion failed: {}", msg),
        PE::StringCacheMismatch(msg) => format!("String cache mismatch: {}", msg),
        PE::SQLInterface(msg) | PE::SQLSyntax(msg) => msg.to_string(),
        PE::Context { error, msg } => {
            let inner = user_message_from_polars(error);
            format!("{}: {}", msg, inner)
        }
        #[allow(unreachable_patterns)]
        _ => err.to_string(),
    }
}

/// Format an io::Error as a user-facing message by matching on ErrorKind.
pub fn user_message_from_io(err: &io::Error, context: Option<&str>) -> String {
    use std::io::ErrorKind;

    let base: String = match err.kind() {
        ErrorKind::NotFound => "File or directory not found.".to_string(),
        ErrorKind::PermissionDenied => "Permission denied. Check read access.".to_string(),
        ErrorKind::ConnectionRefused => "Connection refused.".to_string(),
        ErrorKind::ConnectionReset => "Connection reset.".to_string(),
        ErrorKind::InvalidData | ErrorKind::InvalidInput => {
            "Invalid or corrupted data.".to_string()
        }
        ErrorKind::UnexpectedEof => "Unexpected end of file.".to_string(),
        ErrorKind::WouldBlock => "Operation would block.".to_string(),
        ErrorKind::Interrupted => "Operation interrupted.".to_string(),
        ErrorKind::OutOfMemory => "Out of memory.".to_string(),
        ErrorKind::Other => {
            let msg = err.to_string();
            if msg.contains("No space left") || msg.contains("space left") {
                return "No space left on device. Free up disk space and try again.".to_string();
            }
            if msg.contains("Is a directory") {
                return "Path is a directory, not a file.".to_string();
            }
            return if context.is_some() {
                format!("I/O error: {}", msg)
            } else {
                msg
            };
        }
        _ => err.to_string(),
    };

    if let Some(ctx) = context {
        if !ctx.is_empty() {
            format!("{} {}", base, ctx)
        } else {
            base
        }
    } else {
        base
    }
}

/// Classification for consumers (e.g. Python binding) that map to native exception types.
/// Keeps error-handling logic in one place instead of duplicating in each binding.
#[derive(Debug, Clone, Copy)]
pub enum ErrorKindForPython {
    FileNotFound,
    PermissionDenied,
    Other,
}

/// Classify a report and return a kind plus user-facing message. Used by the Python binding
/// to raise FileNotFoundError, PermissionDenied, or RuntimeError without duplicating chain-walk logic.
pub fn error_for_python(report: &color_eyre::eyre::Report) -> (ErrorKindForPython, String) {
    use std::io::ErrorKind;
    for cause in report.chain() {
        if let Some(io_err) = cause.downcast_ref::<io::Error>() {
            let kind = match io_err.kind() {
                ErrorKind::NotFound => ErrorKindForPython::FileNotFound,
                ErrorKind::PermissionDenied => ErrorKindForPython::PermissionDenied,
                _ => ErrorKindForPython::Other,
            };
            let msg = io_err.to_string();
            return (kind, msg);
        }
    }
    let display = report.to_string();
    let msg = display
        .lines()
        .next()
        .map(str::trim)
        .unwrap_or("An error occurred")
        .to_string();
    (ErrorKindForPython::Other, msg)
}

/// Format a color_eyre Report by downcasting to known error types.
/// Walks the cause chain to find PolarsError or io::Error.
pub fn user_message_from_report(report: &color_eyre::eyre::Report, path: Option<&Path>) -> String {
    for cause in report.chain() {
        if let Some(pe) = cause.downcast_ref::<PolarsError>() {
            let msg = user_message_from_polars(pe);
            return if let Some(p) = path {
                format!("Failed to load {}: {}", p.display(), msg)
            } else {
                msg
            };
        }
        if let Some(io_err) = cause.downcast_ref::<io::Error>() {
            let msg = user_message_from_io(io_err, None);
            return if let Some(p) = path {
                format!("Failed to load {}: {}", p.display(), msg)
            } else {
                msg
            };
        }
    }

    // Fallback: use first line of display to avoid long tracebacks
    let display = report.to_string();
    let first_line = display.lines().next().unwrap_or("An error occurred");
    let trimmed = first_line.trim();
    if let Some(p) = path {
        format!("Failed to load {}: {}", p.display(), trimmed)
    } else {
        trimmed.to_string()
    }
}

/// Polars' own words, with its query plan taken off the end.
///
/// When a scan fails inside a plan, Polars appends the plan it had resolved so far —
/// several lines of `Resolved plan until failure:`, an arrow reading `FAILED HERE
/// RESOLVING THIS_NODE`, and a fragment naming the node. On a terminal that lands in an
/// error modal as a paragraph of internals above the one sentence that matters.
///
/// The one part of it worth keeping is the file the scan stopped at, which the plan
/// names and the message above it usually does not.
fn without_the_query_plan(msg: &str) -> String {
    let Some(cut) = msg.find("Resolved plan until failure:") else {
        return msg.to_string();
    };
    let (said, plan) = msg.split_at(cut);
    let said = said.trim_end();
    match file_in_plan(plan) {
        Some(file) => format!("{said}\nIt stopped at {file}."),
        None => said.to_string(),
    }
}

/// The path in a plan fragment's scan node: `Csv SCAN [/data/one.csv]`.
///
/// The one under the marker, not the first in the plan. A plan with more than one scan
/// — a union branch, a join — names them all, and the first is rarely the one that
/// failed; picking it makes a specific, checkable claim about the wrong file, which is
/// worse than saying nothing.
fn file_in_plan(plan: &str) -> Option<&str> {
    const MARKER: &str = "FAILED HERE";
    let from = plan.find(MARKER).map_or(0, |at| at + MARKER.len());
    let at = plan[from..].find(" SCAN [")? + from + " SCAN [".len();
    let rest = &plan[at..];
    let end = rest.find(']')?;
    let file = rest[..end].trim();
    (!file.is_empty()).then_some(file)
}

/// Files that could not be stacked into one table, said as a folder rather than as a
/// pair of schemas.
///
/// Polars prints both schemas in full — every field and dtype of each — which for two
/// forty-column files is a screen of braces, and neither is labelled with the file it
/// came from. Since #275 the reader unions by name and widens types, so this is what is
/// left when even that cannot reconcile them, and the useful answer is which file and
/// what to do, not the two schemas.
fn is_union_schema_error(msg: &str) -> bool {
    // Only the phrase a multi-file read produces. `unable to vstack` was here too and
    // matched far more than it meant: `DataFrame::vstack` stitches the row buffer and
    // builds a segment in the data-quality pass, both on a single open file with no
    // folder in sight — and this message would have told the user to open one file
    // instead, throwing the real cause away to do it.
    msg.contains("'union'/'concat' inputs should all have the same schema")
}

fn union_schema_message(msg: &str) -> String {
    let mut said = "These files cannot be read as one table: they disagree on a column \
                    in a way datui cannot reconcile by widening its type."
        .to_string();
    if let Some(file) = file_in_plan(msg) {
        said.push_str(&format!("\nIt stopped at {file}."));
    }
    said.push_str(
        "\nTry: open one file on its own, or --format / --infer-schema-length to settle \
         the types.",
    );
    said
}

/// Light cleanup for ComputeError messages: strip Polars-internal phrasing.
fn simplify_compute_message(msg: &str) -> String {
    if is_csv_parse_type_error(msg) {
        return short_csv_parse_error_message(msg);
    }
    crate::query::sanitize_query_error(msg)
}

/// True if this looks like Polars' "could not parse X as dtype Y" / "invalid primitive value" CSV error.
fn is_csv_parse_type_error(msg: &str) -> bool {
    let m = msg.to_lowercase();
    (m.contains("could not parse") && m.contains("as dtype"))
        || m.contains("invalid primitive value")
}

/// Extract column name from Polars message like "at column 'name'" or "at column \"name\"".
fn extract_csv_parse_column(msg: &str) -> Option<String> {
    let m = msg.to_lowercase();
    for (needle, quote) in [("at column '", '\''), ("at column \"", '"')] {
        if let Some(start) = m.find(needle) {
            let after = &msg[start + needle.len()..];
            let end = after.find(quote)?;
            let name = after[..end].trim();
            if !name.is_empty() {
                return Some(name.to_string());
            }
        }
    }
    None
}

fn short_csv_parse_error_message(raw: &str) -> String {
    let col = extract_csv_parse_column(raw);
    let first = match &col {
        Some(c) => format!(
            "CSV parse error in column '{}': a value didn't match the inferred type.",
            c
        ),
        None => "CSV parse error: a value didn't match the inferred column type.".to_string(),
    };
    format!(
        "{}\n\
         Try: --infer-schema-length 1000\n\
              --null-value <value>  (treat as null)\n\
              --ignore-errors true  (skip bad rows)",
        first
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real message datui produced, with Polars' plan on the end of it.
    ///
    /// Captured from a folder of three CSVs that share no columns, before the reader
    /// learned to union them. The plan is four lines of internals around one fact worth
    /// keeping — the file it stopped at.
    #[test]
    fn a_polars_query_plan_is_not_shown_to_the_user() {
        let raw = "Operation not allowed: 'union'/'concat' inputs should all have the \
                   same schema,got\nSchema { fields: {\"a\": Int64, \"b\": Int64} } and \
                   \nSchema { fields: {\"q\": Int64} }\n\nResolved plan until failure:\n\n\
                   \t---> FAILED HERE RESOLVING THIS_NODE <---\nCsv SCAN \
                   [/data/mixed/two.csv]\nPROJECT */3 COLUMNS\nESTIMATED ROWS: 2";

        let said = union_schema_message(raw);
        assert!(
            !said.contains("FAILED HERE") && !said.contains("PROJECT"),
            "the plan is gone: {said:?}"
        );
        assert!(
            !said.contains("Schema {"),
            "and so are two schemas printed in full: {said:?}"
        );
        assert!(
            said.contains("/data/mixed/two.csv"),
            "but the file it stopped at is kept: {said:?}"
        );

        // And the general case, for every other error Polars hangs a plan on.
        let other = "Column not found: region\n\nResolved plan until failure:\n\n\
                     \t---> FAILED HERE RESOLVING THIS_NODE <---\nParquet SCAN \
                     [/data/events/part-7.parquet]\nPROJECT 3/9 COLUMNS";
        let tidied = without_the_query_plan(other);
        assert_eq!(
            tidied, "Column not found: region\nIt stopped at /data/events/part-7.parquet.",
            "got {tidied:?}"
        );

        // A message with no plan on it is untouched.
        assert_eq!(
            without_the_query_plan("Column not found: region"),
            "Column not found: region"
        );

        // More than one scan in the plan: the one under the marker, not the first.
        let two_scans = "Column not found: region\n\nResolved plan until failure:\n\n                         Parquet SCAN [/data/a.parquet]\nUNION\n                         \t---> FAILED HERE RESOLVING THIS_NODE <---\n                         Csv SCAN [/data/b.csv]";
        assert!(
            without_the_query_plan(two_scans).ends_with("It stopped at /data/b.csv."),
            "got {:?}",
            without_the_query_plan(two_scans)
        );

        // `unable to vstack` is not a folder problem: the row buffer and the
        // data-quality pass both stitch frames of one open file with it.
        assert!(
            !is_union_schema_error("unable to vstack, column names don't match: \"a\" and \"b\""),
            "a single-file vstack must keep its own message"
        );
    }

    #[test]
    fn test_user_message_from_io_not_found() {
        let err = io::Error::new(io::ErrorKind::NotFound, "No such file");
        let msg = user_message_from_io(&err, None);
        assert!(
            msg.contains("not found"),
            "expected 'not found', got: {}",
            msg
        );
    }

    #[test]
    fn test_user_message_from_io_permission_denied() {
        let err = io::Error::new(io::ErrorKind::PermissionDenied, "Permission denied");
        let msg = user_message_from_io(&err, None);
        assert!(
            msg.to_lowercase().contains("permission"),
            "expected 'permission', got: {}",
            msg
        );
    }

    #[test]
    fn test_user_message_from_polars_column_not_found() {
        use polars::prelude::PolarsError;
        let err = PolarsError::ColumnNotFound("foo".into());
        let msg = user_message_from_polars(&err);
        assert!(msg.contains("foo"), "expected 'foo', got: {}", msg);
        assert!(
            msg.contains("Column not found"),
            "expected column not found, got: {}",
            msg
        );
    }

    #[test]
    fn test_user_message_from_polars_duplicate() {
        use polars::prelude::PolarsError;
        let err = PolarsError::Duplicate("bar".into());
        let msg = user_message_from_polars(&err);
        assert!(
            msg.contains("Duplicate"),
            "expected 'Duplicate', got: {}",
            msg
        );
        assert!(msg.contains("alias"), "expected alias hint, got: {}", msg);
    }

    #[test]
    fn test_simplify_compute_message_alias_hint() {
        let raw = "projections contained duplicate: 'x'. Try renaming with .alias(\"name\")";
        let msg = simplify_compute_message(raw);
        assert!(
            !msg.contains(".alias("),
            "should strip .alias( hint: {}",
            msg
        );
        assert!(
            msg.contains("Use aliases"),
            "expected alias suggestion: {}",
            msg
        );
    }

    #[test]
    fn test_simplify_compute_message_csv_parse_error() {
        let raw = "could not parse `N/A` as dtype `i64` at column 'column' (column number 1)\n\n\
            The current offset in the file is 292 bytes.\n\n\
            You might want to try: ...\n\
            Original error: ```invalid primitive value found during CSV parsing```";
        let msg = simplify_compute_message(raw);
        assert!(
            msg.contains("CSV parse error"),
            "expected short CSV message: {}",
            msg
        );
        assert!(
            msg.contains("column 'column'"),
            "expected offending column in message: {}",
            msg
        );
        assert!(
            msg.contains("--infer-schema-length"),
            "expected CLI hint: {}",
            msg
        );
        assert!(
            msg.contains("--null-value"),
            "expected null-value hint: {}",
            msg
        );
        assert!(
            !msg.contains("Original error"),
            "should not regurgitate Polars: {}",
            msg
        );
    }
}
