//! User-facing error message formatting.
//!
//! Uses typed error matching (PolarsError variants, io::ErrorKind) rather than
//! string parsing to produce actionable, implementation-agnostic messages.

use polars::prelude::PolarsError;
use std::io;
use std::path::Path;

/// Format a PolarsError as a user-facing message by matching on its variant.
pub fn user_message_from_polars(err: &PolarsError) -> String {
    // Polars' words first, then the tidying every one of them wants: its query plan taken
    // off the end, and the one shape worth rewriting said as a directory rather than as
    // two schemas printed in full. Done here, around the match, so no arm can be added
    // that forgets it.
    let said = polars_words(err);
    if is_union_schema_error(&said) {
        return union_schema_message(&said);
    }
    if let Some(differ) = files_columns_differ(&said) {
        return differ;
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

/// Values that would not convert, from a strict CAST or a STRPTIME, as the failed
/// run reported them: Polars counts the failures in the batch it was converting and
/// quotes a few.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversionFailure {
    pub column: String,
    /// The target type as Polars spells it: `i32`, `date`, `datetime[μs]`.
    pub to: String,
    pub failed: usize,
    /// Values in the batch the failures were counted in. The whole column only when
    /// it fit in one batch.
    pub checked: usize,
    /// Distinct offending values, at most three, without their quotes.
    pub examples: Vec<String>,
    /// Parsed with a format (STRPTIME) rather than cast.
    pub parsing: bool,
}

/// The conversion that failed, if that is what `err` is.
pub fn conversion_failure(err: &PolarsError) -> Option<ConversionFailure> {
    let mut parsing = false;
    let mut err = err;
    loop {
        match err {
            PolarsError::ExprContext { error, expr } => {
                parsing |= expr.contains("strptime") || expr.contains("to_date");
                err = error;
            }
            PolarsError::Context { error, .. } => err = error,
            _ => break,
        }
    }
    let PolarsError::InvalidOperation(msg) = err else {
        return None;
    };
    parse_conversion(msg, parsing)
}

/// `conversion from `str` to `i32` failed in column 'FT' for 8 out of 380 values:
/// ["n/a", "n/a", … "n/a"]`, the shape `handle_casting_failures` writes.
fn parse_conversion(msg: &str, parsing: bool) -> Option<ConversionFailure> {
    let rest = msg.strip_prefix("conversion from `")?;
    let (_, rest) = rest.split_once("` to `")?;
    let (to, rest) = rest.split_once("` failed in column '")?;
    let (column, rest) = rest.split_once("' for ")?;
    let (failed, rest) = rest.split_once(" out of ")?;
    let (checked, rest) = rest.split_once(" values: ")?;
    let list = rest.lines().next().unwrap_or("");
    let list = list.strip_prefix('[').unwrap_or(list);
    let list = list.strip_suffix(']').unwrap_or(list);
    let mut examples: Vec<String> = Vec::new();
    for item in list.split(", ") {
        // The truncation marker Polars puts before the last value.
        let item = item.trim_start_matches('…').trim();
        let item = item
            .strip_prefix('"')
            .and_then(|i| i.strip_suffix('"'))
            .unwrap_or(item);
        if !item.is_empty() && !examples.iter().any(|e| e == item) && examples.len() < 3 {
            examples.push(item.to_string());
        }
    }
    Some(ConversionFailure {
        column: column.to_string(),
        to: to.to_string(),
        failed: failed.trim().parse().ok()?,
        checked: checked.trim().parse().ok()?,
        examples,
        parsing,
    })
}

impl ConversionFailure {
    /// What a SQL writer can act on: the column, how many values, a few of them, and
    /// the SQL that would get past them. `rows` is how many rows `df` holds, when
    /// known. The count is "N of M" only when Polars checked all of them; otherwise it
    /// covers the batch the run stopped in, and is said as a lower bound.
    pub fn sql_message(&self, rows: Option<usize>) -> String {
        let column = crate::sql_assist::sql_name(&self.column);
        let exact = rows == Some(self.checked);
        let one = !exact && self.failed == 1;
        let temporal = self.to == "date" || self.to == "time" || self.to.starts_with("datetime");
        let (many, single) = if temporal && self.parsing {
            ("do not match the format", "does not match the format")
        } else if self.to == "date" {
            (
                "are not dates written YYYY-MM-DD",
                "is not a date written YYYY-MM-DD",
            )
        } else if self.to == "time" {
            (
                "are not times written HH:MM:SS",
                "is not a time written HH:MM:SS",
            )
        } else if temporal {
            (
                "are not timestamps written YYYY-MM-DD HH:MM:SS",
                "is not a timestamp written YYYY-MM-DD HH:MM:SS",
            )
        } else if self.to.starts_with('i') || self.to.starts_with('u') {
            ("are not whole numbers", "is not a whole number")
        } else if self.to.starts_with('f') || self.to.starts_with("decimal") {
            ("are not numbers", "is not a number")
        } else if self.to == "bool" {
            ("are not true or false", "is not true or false")
        } else {
            ("cannot be converted", "cannot be converted")
        };
        let quoted: Vec<String> = self.examples.iter().map(|e| format!("\"{e}\"")).collect();
        let such_as = match quoted.as_slice() {
            [] => String::new(),
            [one] => format!(", such as {one}"),
            [init @ .., last] => format!(", such as {} and {last}", init.join(", ")),
        };
        let lead = if exact {
            format!(
                "{column}: {} of {} values {many}{such_as}.",
                crate::numfmt::group_chrome(self.failed),
                crate::numfmt::group_chrome(self.checked)
            )
        } else if one {
            format!("At least 1 value in {column} {single}{such_as}.")
        } else {
            format!(
                "At least {} values in {column} {many}{such_as}.",
                crate::numfmt::group_chrome(self.failed)
            )
        };
        let hint = if temporal && self.parsing {
            format!(
                "Try: a format that fits them all, or trim the text first with \
                 SUBSTR({column}, 1, n) or REPLACE({column}, 'text', '')."
            )
        } else if temporal {
            format!(
                "Try: STRPTIME({column}, '%d/%m/%Y') with the format the values are \
                 written in."
            )
        } else {
            let sql_type = sql_type_for(&self.to);
            format!(
                "Try: TRY_CAST({column} AS {sql_type}) to read them as null, or clean \
                 the text first with REPLACE or SUBSTR."
            )
        };
        format!("{lead}\n{hint}")
    }
}

/// The SQL type name for a Polars one, for a TRY_CAST suggestion.
fn sql_type_for(polars: &str) -> &'static str {
    match polars {
        "i8" => "TINYINT",
        "i16" => "SMALLINT",
        "i32" => "INT",
        "i64" => "BIGINT",
        "i128" => "HUGEINT",
        "u8" => "UTINYINT",
        "u16" => "USMALLINT",
        "u32" => "UINTEGER",
        "u64" => "UBIGINT",
        "f32" => "REAL",
        "f64" => "DOUBLE",
        "bool" => "BOOLEAN",
        t if t.starts_with("decimal") => "DECIMAL",
        _ => "VARCHAR",
    }
}

/// A SQL error as the query prompt shows it: a failed conversion in datui's words,
/// anything else as Polars says it.
pub fn sql_error_message(err: &PolarsError, rows: Option<usize>) -> String {
    match conversion_failure(err) {
        Some(failure) => failure.sql_message(rows),
        None => user_message_from_polars(err),
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

/// `message` with `file`, a temporary copy datui made, called `source`: what the user
/// opened. A download or a decompressed CSV is read from a temp path the user never
/// typed, and Polars names the file it was reading.
///
/// Only a whole mention is replaced: one that a path character neither precedes nor
/// follows, so a longer path that merely starts or ends with the temp file's (its
/// name plus an extension, the same name in another directory) is left as it is. A
/// full stop that ends a sentence still ends the mention.
pub fn named_by_source(message: &str, file: &Path, source: &Path) -> String {
    let file = file.to_string_lossy();
    if file.is_empty() {
        return message.to_string();
    }
    let source = source.to_string_lossy();
    let in_a_name = |c: char| c.is_alphanumeric() || matches!(c, '_' | '-' | '~');
    let in_a_path = |c: char| in_a_name(c) || matches!(c, '.' | '/' | '\\');
    let mut named = String::with_capacity(message.len());
    let mut copied = 0;
    for (at, _) in message.match_indices(file.as_ref()) {
        let end = at + file.len();
        let mut after = message[end..].chars();
        let whole = !message[..at].chars().next_back().is_some_and(in_a_path)
            && match after.next() {
                None => true,
                Some('.') => !after.next().is_some_and(in_a_path),
                Some(c) => !in_a_path(c),
            };
        if whole {
            named.push_str(&message[copied..at]);
            named.push_str(&source);
            copied = end;
        }
    }
    named.push_str(&message[copied..]);
    named
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

/// Files that could not be stacked into one table, said as a directory rather than as a
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
    // directory in sight — and this message would have told the user to open one file
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

/// Files whose columns could not be lined up, said as files rather than as schemas.
///
/// Polars says this merging the schemas it inferred from each file of a directory:
/// `schema names differ: got 39, expected 25`, where 39 and 25 are column *names* — the
/// first row of a file with no header, read as one. Read as counts, it points nowhere.
fn files_columns_differ(msg: &str) -> Option<String> {
    let how = if let Some(rest) = msg.split("schema names differ: got ").nth(1) {
        let (got, expected) = rest.split_once(", expected ")?;
        let expected = expected.lines().next().unwrap_or(expected).trim();
        format!(
            "one has a column named `{}` where another has `{expected}`",
            got.trim()
        )
    } else if msg.contains("schema lengths differ") {
        "they have different numbers of columns".to_string()
    } else {
        return None;
    };
    Some(format!(
        "These files cannot be read as one table: their columns differ — {how}.\n\
         Try: open one file on its own. If the files have no header row, --no-header \
         reads their columns by position."
    ))
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
              --ignore-errors  (skip bad rows)",
        first
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A temp copy is named by what the user opened, wherever the message says it,
    /// and a message that does not mention it is left alone.
    #[test]
    fn a_temp_copy_is_named_by_its_source() {
        let file = Path::new("/home/u/tmp/.tmp9tY5X2.parquet");
        let url = Path::new("http://host/broken.parquet");
        let said = named_by_source(
            "Failed to load /home/u/tmp/.tmp9tY5X2.parquet: bad\nIt stopped at /home/u/tmp/.tmp9tY5X2.parquet.",
            file,
            url,
        );
        assert_eq!(
            said,
            "Failed to load http://host/broken.parquet: bad\nIt stopped at http://host/broken.parquet."
        );
        assert_eq!(named_by_source("no path here", file, url), "no path here");
        assert_eq!(named_by_source("x", Path::new(""), url), "x");
        assert_eq!(
            named_by_source("'/home/u/tmp/.tmp9tY5X2.parquet' (os error 2)", file, url),
            "'http://host/broken.parquet' (os error 2)"
        );
    }

    /// A path that only shares the temp file's as its start or its end is another
    /// file, and is left alone; so is the same name under another directory.
    #[test]
    fn a_similar_path_is_not_renamed() {
        let copy = Path::new("/home/u/tmp/.tmpAb12Cd");
        let gz = Path::new("/data/rows.csv.gz");
        for other in [
            "/home/u/tmp/.tmpAb12Cd.csv",
            "/home/u/tmp/.tmpAb12Cd2",
            "/home/u/tmp/.tmpAb12Cd_old",
            "/home/u/tmp/.tmpAb12Cd/part-0.csv",
            "/mnt/home/u/tmp/.tmpAb12Cd",
            "x/home/u/tmp/.tmpAb12Cd",
        ] {
            let message = format!("Failed to load {other}: bad");
            assert_eq!(named_by_source(&message, copy, gz), message, "{other}");
        }
        assert_eq!(
            named_by_source(
                "/home/u/tmp/.tmpAb12Cd.csv is not /home/u/tmp/.tmpAb12Cd.",
                copy,
                gz
            ),
            "/home/u/tmp/.tmpAb12Cd.csv is not /data/rows.csv.gz.",
        );
    }

    /// The census directory in `cloud-samples-data`: two headerless CSVs and one with a
    /// header, so the names Polars compares are a first row's values.
    #[test]
    fn files_whose_columns_differ_are_said_as_files() {
        let said = user_message_from_polars(&PolarsError::ComputeError(
            "schema names differ: got 39, expected 25".into(),
        ));
        assert!(said.contains("cannot be read as one table"), "{said}");
        assert!(
            said.contains("a column named `39` where another has `25`"),
            "{said}"
        );
        assert!(said.contains("--no-header"), "{said}");
        assert!(!said.contains("schema"), "no Polars words left: {said}");

        let said =
            user_message_from_polars(&PolarsError::ComputeError("schema lengths differ".into()));
        assert!(said.contains("different numbers of columns"), "{said}");

        let other =
            user_message_from_polars(&PolarsError::ComputeError("something else entirely".into()));
        assert!(!other.contains("one table"), "{other}");
    }

    /// A real message datui produced, with Polars' plan on the end of it.
    ///
    /// Captured from a directory of three CSVs that share no columns, before the reader
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

        // `unable to vstack` is not a directory problem: the row buffer and the
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

    /// A SQL statement's failure as the run reports it.
    #[cfg(feature = "sql")]
    fn sql_failure(sql: &str, df: polars::prelude::DataFrame) -> PolarsError {
        use polars::prelude::IntoLazy;
        let mut ctx = polars_sql::SQLContext::new();
        ctx.register("df", df.lazy());
        ctx.execute(sql)
            .expect("plans")
            .collect()
            .expect_err("fails at run time")
    }

    /// The Premier League date column: a few postponed matches carry a marker.
    #[cfg(feature = "sql")]
    fn matches() -> polars::prelude::DataFrame {
        let dates: Vec<String> = (0..380)
            .map(|i| match i % 30 {
                0 => "Tue Jan 12 2021(P)".to_string(),
                10 => "Sat Feb 20 2021(P)".to_string(),
                _ => "Sun Sep 13 2020".to_string(),
            })
            .collect();
        let scores: Vec<String> = (0..380)
            .map(|i| if i % 50 == 0 { "n/a" } else { "3" }.to_string())
            .collect();
        polars::prelude::df!("Date" => dates, "Team 1" => scores).unwrap()
    }

    #[cfg(feature = "sql")]
    #[test]
    fn a_date_that_does_not_parse_is_said_in_sql_terms() {
        let err = sql_failure(
            "SELECT STRPTIME(Date, '%a %b %d %Y') AS d FROM df",
            matches(),
        );
        let failure = conversion_failure(&err).expect("a conversion");
        assert_eq!(failure.column, "Date");
        assert!(failure.parsing);
        assert_eq!(failure.failed, 26);
        let msg = sql_error_message(&err, Some(380));
        assert!(
            msg.starts_with(
                "Date: 26 of 380 values do not match the format, such as \"Tue Jan 12 2021(P)\""
            ),
            "{msg}"
        );
        assert!(msg.contains("SUBSTR(Date, 1, n)"), "{msg}");
        assert!(
            !msg.contains("strict=False") && !msg.contains("str.strptime"),
            "{msg}"
        );
        // Read in batches, the count is a floor.
        let msg = sql_error_message(&err, Some(1000));
        assert!(
            msg.starts_with("At least 26 values in Date do not match"),
            "{msg}"
        );
    }

    #[cfg(feature = "sql")]
    #[test]
    fn a_cast_that_fails_names_the_column_and_suggests_try_cast() {
        let err = sql_failure(
            "SELECT CAST(\"Team 1\" AS INT) + 1 AS goals FROM df ORDER BY goals",
            matches(),
        );
        let msg = sql_error_message(&err, Some(380));
        assert!(
            msg.starts_with("\"Team 1\": 8 of 380 values are not whole numbers, such as \"n/a\"."),
            "{msg}"
        );
        assert!(msg.contains("TRY_CAST(\"Team 1\" AS INT)"), "{msg}");
    }

    /// Without the whole table in the batch, one failure is a floor too.
    #[test]
    fn a_count_short_of_the_table_is_a_lower_bound() {
        let failure = ConversionFailure {
            column: "FT".to_string(),
            to: "i32".to_string(),
            failed: 1,
            checked: 1,
            examples: vec!["0–3".to_string()],
            parsing: false,
        };
        let lead = |rows| {
            failure
                .sql_message(rows)
                .lines()
                .next()
                .unwrap()
                .to_string()
        };
        assert_eq!(
            lead(None),
            "At least 1 value in FT is not a whole number, such as \"0–3\"."
        );
        assert_eq!(
            lead(Some(1)),
            "FT: 1 of 1 values are not whole numbers, such as \"0–3\"."
        );
    }

    #[test]
    fn anything_else_is_said_as_polars_says_it() {
        let err = PolarsError::InvalidOperation("something else".into());
        assert_eq!(conversion_failure(&err), None);
        assert_eq!(
            sql_error_message(&err, None),
            user_message_from_polars(&err)
        );
    }
}
