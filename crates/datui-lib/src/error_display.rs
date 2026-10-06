//! User-facing error message formatting.
//!
//! Uses typed error matching (PolarsError variants, io::ErrorKind) rather than
//! string parsing to produce actionable, implementation-agnostic messages.

use polars::prelude::PolarsError;
use std::io;
use std::path::{Path, PathBuf};

/// An error reading a file, said as every reader error is:
/// `"<path>": <what went wrong>. <what to do>.` ([`file_message`]).
#[derive(Debug)]
pub struct FileError {
    path: PathBuf,
    /// Line and column, one-based, when the problem is at one place in the file's text.
    at: Option<(usize, usize)>,
    what: String,
    /// What it was told, so a cause (a missing file) is still found under it.
    source: Option<color_eyre::eyre::Report>,
}

impl FileError {
    pub fn new(path: &Path, what: impl Into<String>) -> Self {
        Self {
            path: path.to_path_buf(),
            at: None,
            what: what.into(),
            source: None,
        }
    }

    /// The problem at `line`:`column` (one-based; line 0 is nowhere in particular),
    /// said as a compiler does: `"spec.toml":3:7: …`.
    pub fn at(path: &Path, line: usize, column: usize, what: impl Into<String>) -> Self {
        Self {
            at: (line > 0).then_some((line, column)),
            ..Self::new(path, what)
        }
    }
}

impl std::fmt::Display for FileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&located_message(Some(&self.path), self.at, &self.what))
    }
}

impl std::error::Error for FileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| &**e as &(dyn std::error::Error + 'static))
    }
}

/// `what` went wrong reading `path`, in the one shape reader errors take:
/// `"<path>": <what went wrong>. <what to do>.` Sentence case, its first line ended
/// with a full stop, the file named once.
pub fn file_message(path: &Path, what: &str) -> String {
    located_message(Some(path), None, what)
}

/// Whether `what` starts by naming a file, as [`file_message`] does: `"<path>": ` or
/// `"<path>":3:7: `. A shard's error said under its dataset, or a spec's under the
/// file it was to read, keeps the file it names.
fn names_a_file(what: &str) -> bool {
    let Some(quoted) = what.strip_prefix('"') else {
        return false;
    };
    let Some((_, after)) = quoted.split_once("\":") else {
        return false;
    };
    after.starts_with(' ') || after.starts_with(|c: char| c.is_ascii_digit())
}

/// [`file_message`], with the line and column the problem is at, when it is at one
/// place: `"spec.toml":3:7: <what went wrong>.` Without a path, the location and the
/// sentence alone.
pub fn located_message(path: Option<&Path>, at: Option<(usize, usize)>, what: &str) -> String {
    let mut said = String::with_capacity(what.len() + 16);
    let mut what = what.trim();
    if let Some(path) = path {
        if names_a_file(what) {
            return what.to_string();
        }
        let named = path.display().to_string();
        // A message that already names the file, as a path does, is not named twice.
        what = what.strip_prefix(&format!("{named}: ")).unwrap_or(what);
        said.push('"');
        said.push_str(&named);
        said.push('"');
        said.push(':');
        if at.is_none() {
            said.push(' ');
        }
    }
    if let Some((line, column)) = at {
        said.push_str(&format!("{line}:{column}: "));
    }
    said.push_str(&sentence(what));
    said
}

/// `what` as a sentence: its first letter capitalized and its first line ended with a
/// full stop. The lines after it are kept as they are.
pub fn sentence(what: &str) -> String {
    let what = what.trim();
    let (first, rest) = what.split_once('\n').unwrap_or((what, ""));
    let first = first.trim_end();
    let mut said = String::with_capacity(what.len() + 1);
    let mut chars = first.chars();
    if let Some(c) = chars.next() {
        // A key or a name is written as the user wrote it, to be searched for.
        if starts_with_a_key(first) {
            said.push(c);
        } else {
            said.extend(c.to_uppercase());
        }
        said.push_str(chars.as_str());
    }
    if !first.ends_with(['.', '?', '!']) {
        said.push('.');
    }
    if !rest.is_empty() {
        said.push('\n');
        said.push_str(rest);
    }
    said
}

/// Whether `what` starts with a key, a path or a name rather than a word: its first
/// word followed by `:` (`type: expected …`), or holding `.`, `_`, `=`, a digit, a
/// quote or a backtick (`tags.nine`, `"day"`). [`sentence`] leaves it as written.
pub fn starts_with_a_key(what: &str) -> bool {
    let word = what.split_whitespace().next().unwrap_or_default();
    word.ends_with(':')
        || word.contains(|c: char| matches!(c, '.' | '_' | '=' | '"' | '`') || c.is_ascii_digit())
}

/// What an object store said about a file in it: a missing object, refused access, or
/// the store's own words, each with what to check.
#[cfg(feature = "cloud")]
pub fn store_message(err: &object_store::Error) -> String {
    use object_store::Error as E;
    match err {
        E::NotFound { .. } => "No object there. Check the URL.".to_string(),
        E::PermissionDenied { .. } => "Access denied. Check the credentials.".to_string(),
        E::Unauthenticated { .. } => {
            "The store did not accept the credentials. Check them.".to_string()
        }
        e => format!(
            "Could not read it: {}. Check the credentials and the URL.",
            e.to_string().trim_end_matches('.')
        ),
    }
}

/// What an HTTP request for `url` came to, when it failed: the server's answer, or
/// that there was none, naming the host either way.
#[cfg(any(feature = "http", feature = "cloud"))]
pub fn http_message(url: &str, err: &ureq::Error) -> String {
    let host = url_host(url);
    match err {
        ureq::Error::StatusCode(code @ (404 | 410)) => {
            format!("The server at {host} returned {code}: the file may have moved.")
        }
        ureq::Error::StatusCode(code @ (401 | 403)) => {
            format!("The server at {host} refused it ({code}). Check the URL and its access.")
        }
        ureq::Error::StatusCode(code @ 500..=599) => {
            format!("The server at {host} returned {code}: try again later.")
        }
        ureq::Error::StatusCode(code) => format!("The server at {host} returned {code}."),
        _ if http_unanswered(err) => format!("No answer from {host}."),
        e => format!(
            "Could not read it: {}.",
            e.to_string().trim_end_matches('.')
        ),
    }
}

/// An HTTP(S) file a request settled cannot be had.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpGone {
    /// What its home row says in place of a size: `HTTP 404`, or `no answer`.
    pub cell: String,
    /// The sentence: [`http_message`].
    pub message: String,
}

/// `err` as [`HttpGone`] when it settles that `url` cannot be had: the file is not
/// there, or no server answered. `None` for what an open might still get past, such as
/// a refused HEAD or a server error.
#[cfg(any(feature = "http", feature = "cloud"))]
pub fn http_gone(url: &str, err: &ureq::Error) -> Option<HttpGone> {
    let cell = match err {
        ureq::Error::StatusCode(code @ (404 | 410)) => format!("HTTP {code}"),
        _ if http_unanswered(err) => "no answer".to_string(),
        _ => return None,
    };
    Some(HttpGone {
        cell,
        message: http_message(url, err),
    })
}

/// No server answered: its name did not resolve, nothing listened, or it said nothing
/// in time.
#[cfg(any(feature = "http", feature = "cloud"))]
fn http_unanswered(err: &ureq::Error) -> bool {
    matches!(
        err,
        ureq::Error::HostNotFound
            | ureq::Error::ConnectionFailed
            | ureq::Error::Timeout(_)
            | ureq::Error::Io(_)
    )
}

/// The host of `url` for a message, with its port when it names one; the URL itself
/// when it has no host.
#[cfg(any(feature = "http", feature = "cloud"))]
fn url_host(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|u| {
            let host = u.host_str()?.to_string();
            Some(match u.port() {
                Some(port) => format!("{host}:{port}"),
                None => host,
            })
        })
        .unwrap_or_else(|| url.to_string())
}

/// `err`, from reading `path`, named by it ([`FileError`]) unless it already is.
pub fn in_file(path: &Path, err: color_eyre::eyre::Report) -> color_eyre::eyre::Report {
    if err.downcast_ref::<FileError>().is_some() {
        return err;
    }
    let what = report_message(cfg!(windows), &err, None);
    color_eyre::eyre::Report::new(FileError {
        source: Some(err),
        ..FileError::new(path, what)
    })
}

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
    if let Some(none) = nothing_matched(&said) {
        return none;
    }
    let said = without_the_query_plan(&said);
    let (first, rest) = said.split_once('\n').unwrap_or((&said, ""));
    match rust_names_said_plainly(first) {
        Some(plain) if rest.is_empty() => plain,
        Some(plain) => format!("{plain}\n{rest}"),
        None => said,
    }
}

/// A file reader's words that are a Rust name (`Out-of-spec: InvalidFooter`,
/// `OutOfSpec`, `InvalidUtf8 at character 0`), said in English.
fn rust_names_said_plainly(msg: &str) -> Option<String> {
    // `InvalidFooter` as `invalid footer`.
    let words = |name: &str| {
        let mut out = String::new();
        for (i, c) in name.chars().enumerate() {
            if c.is_uppercase() && i > 0 {
                out.push(' ');
            }
            out.extend(c.to_lowercase());
        }
        out
    };
    let msg = msg.trim();
    let is_name = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric());
    let damaged = |what: Option<String>| {
        let what = what.map(|w| format!(" ({w})")).unwrap_or_default();
        format!(
            "The file is not laid out as its format says{what}: it is damaged, or another \
             format. --format names the format to read it as."
        )
    };
    if msg == "OutOfSpec" {
        return Some(damaged(None));
    }
    const SPEC: &str = "out-of-spec: ";
    if let Some(name) = msg
        .get(..SPEC.len())
        .filter(|p| p.eq_ignore_ascii_case(SPEC))
        .map(|_| &msg[SPEC.len()..])
        && is_name(name)
    {
        return Some(damaged(Some(words(name))));
    }
    let at = msg.strip_prefix("InvalidUtf8")?;
    Some(format!("The file is not UTF-8 text{at}."))
}

/// A scan whose path expanded to no files, said plainly. Polars prints its expansion
/// input (`paths: [PlRefPath { inner: … }]`, `glob: true`), which reads as internals.
/// A local path is only handed over as a pattern when no file has its name
/// (`source::expands_as_glob`), so for a pattern this is the whole story.
fn nothing_matched(msg: &str) -> Option<String> {
    let (_, input) = msg.split_once("expanded paths were empty")?;
    // The paths are Debug-printed, `paths: [PlRefPath { inner: "…" }]`: look inside
    // the quotes, past the list's own brackets.
    let pattern = input.contains("glob: true")
        && input.split("inner: \"").skip(1).any(|p| {
            p.split('"')
                .next()
                .is_some_and(|p| p.contains(['*', '?', '[']))
        });
    Some(if pattern {
        "No files match this pattern.".to_string()
    } else {
        "No files found there.".to_string()
    })
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

/// What a file another program holds says after its name.
const HELD: &str =
    "is open in another program that does not allow reading it; close it there and reopen";

/// Whether `err` is Windows refusing a file another program holds: a sharing
/// violation (32), as from a spreadsheet app with the workbook open, or a locked
/// region (33). Polars rewraps an open's error with the path in its text, keeping
/// the OS's words but not the code, so the text is read too. Off Windows those
/// codes mean something else.
pub fn held_by_another_program(err: &io::Error) -> bool {
    held_on(cfg!(windows), err)
}

fn held_on(windows: bool, err: &io::Error) -> bool {
    windows && (matches!(err.raw_os_error(), Some(32 | 33)) || says_held(&err.to_string()))
}

/// Whether an error's text carries the code of a file another program holds.
fn says_held(text: &str) -> bool {
    text.contains("(os error 32)") || text.contains("(os error 33)")
}

/// Whether anything in `report` is a file another program holds: an `io::Error`, one
/// inside a Polars error, or, on `windows`, the text of one an error turned into words.
fn report_held(windows: bool, report: &color_eyre::eyre::Report) -> bool {
    fn polars_held(windows: bool, err: &PolarsError) -> bool {
        match err {
            PolarsError::IO { error, .. } => held_on(windows, error),
            PolarsError::Context { error, .. } => polars_held(windows, error),
            _ => false,
        }
    }
    windows
        && report.chain().any(|cause| {
            cause
                .downcast_ref::<io::Error>()
                .is_some_and(|e| held_on(windows, e))
                || cause
                    .downcast_ref::<PolarsError>()
                    .is_some_and(|e| polars_held(windows, e))
                || says_held(&cause.to_string())
        })
}

/// What a file another program holds says, named by `path` when it is known.
fn held_message(path: Option<&Path>) -> String {
    match path {
        Some(path) => file_message(path, &format!("the file {HELD}")),
        None => format!("The file {HELD}."),
    }
}

/// Format an io::Error as a user-facing message by matching on ErrorKind.
pub fn user_message_from_io(err: &io::Error, context: Option<&str>) -> String {
    use std::io::ErrorKind;

    if held_by_another_program(err) {
        return held_message(None);
    }
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
    let named = report.downcast_ref::<FileError>().map(ToString::to_string);
    for cause in report.chain() {
        if let Some(io_err) = cause.downcast_ref::<io::Error>() {
            let kind = match io_err.kind() {
                ErrorKind::NotFound => ErrorKindForPython::FileNotFound,
                ErrorKind::PermissionDenied => ErrorKindForPython::PermissionDenied,
                _ => ErrorKindForPython::Other,
            };
            let msg = named.unwrap_or_else(|| io_err.to_string());
            return (kind, msg);
        }
    }
    if let Some(named) = named {
        return (ErrorKindForPython::Other, named);
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
    report_message(cfg!(windows), report, path)
}

fn report_message(windows: bool, report: &color_eyre::eyre::Report, path: Option<&Path>) -> String {
    // A reader's error names the file it was reading, which may be one of several.
    if let Some(named) = report.downcast_ref::<FileError>() {
        return named.to_string();
    }
    if report_held(windows, report) {
        return held_message(path);
    }
    let named = |msg: String| match path {
        Some(p) => file_message(p, &msg),
        None => msg,
    };
    for cause in report.chain() {
        if let Some(named_file) = cause.downcast_ref::<FileError>() {
            return named_file.to_string();
        }
        if let Some(pe) = cause.downcast_ref::<PolarsError>() {
            return named(user_message_from_polars(pe));
        }
        if let Some(io_err) = cause.downcast_ref::<io::Error>() {
            return named(user_message_from_io(io_err, None));
        }
    }

    // Fallback: use first line of display to avoid long tracebacks
    let display = report.to_string();
    let first_line = display.lines().next().unwrap_or("An error occurred").trim();
    named(rust_names_said_plainly(first_line).unwrap_or_else(|| first_line.to_string()))
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
        "\nTry: open one file on its own, or --format / --infer-rows to settle \
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
            "one has a column named \"{}\" where another has \"{expected}\"",
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
            "CSV parse error in column \"{}\": a value didn't match the inferred type.",
            c
        ),
        None => "CSV parse error: a value didn't match the inferred column type.".to_string(),
    };
    format!(
        "{}\n\
         Try: --infer-rows 1000\n\
              --null <value>  (treat as null)\n\
              --ignore-errors  (skip bad rows)",
        first
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sentence starts with a capital, unless it starts with a key or a name the
    /// user wrote, which is kept as written.
    #[test]
    fn keys_keep_their_case() {
        for (what, said) in [
            ("not a WAV file", "Not a WAV file."),
            ("could not read it: gone", "Could not read it: gone."),
            ("type: expected u1", "type: expected u1."),
            (
                "tags.nine: expected a tag number",
                "tags.nine: expected a tag number.",
            ),
            (
                "header_rows: missing `name`",
                "header_rows: missing `name`.",
            ),
            (
                "\"day\" is made from \"date\"",
                "\"day\" is made from \"date\".",
            ),
            ("u9 is not a type", "u9 is not a type."),
            ("`colour` is not a key", "`colour` is not a key."),
        ] {
            assert_eq!(sentence(what), said, "{what}");
        }
        assert_eq!(
            located_message(
                Some(Path::new("d.toml")),
                Some((3, 1)),
                "tags.x: expected a tag number"
            ),
            "\"d.toml\":3:1: tags.x: expected a tag number."
        );
    }

    /// A reader's error names its file in quotes, once, in sentence case, ended.
    #[test]
    fn a_file_error_names_the_file_once() {
        let path = Path::new("/d/a.wav");
        for what in [
            "not a WAV file",
            "Not a WAV file.",
            "/d/a.wav: not a WAV file",
            "\"/d/a.wav\": Not a WAV file.",
        ] {
            assert_eq!(file_message(path, what), "\"/d/a.wav\": Not a WAV file.");
        }
        assert_eq!(
            file_message(path, "bad\nTry: --format csv"),
            "\"/d/a.wav\": Bad.\nTry: --format csv"
        );
        // Named once however often it passes through, and its cause is still found.
        let err = in_file(path, color_eyre::eyre::eyre!("too short"));
        let err = in_file(Path::new("/d"), err);
        assert_eq!(err.to_string(), "\"/d/a.wav\": Too short.");
        assert_eq!(
            user_message_from_report(&err, Some(Path::new("/elsewhere"))),
            "\"/d/a.wav\": Too short."
        );
        let missing = in_file(path, io::Error::new(io::ErrorKind::NotFound, "gone").into());
        assert!(matches!(
            error_for_python(&missing),
            (ErrorKindForPython::FileNotFound, ref m) if m.starts_with("\"/d/a.wav\": ")
        ));
        assert_eq!(
            user_message_from_report(&color_eyre::eyre::eyre!("bad header"), Some(path)),
            "\"/d/a.wav\": Bad header."
        );
    }

    /// A reader's Rust names are said in English.
    #[test]
    fn rust_names_are_said_plainly() {
        let said = |m: &str| rust_names_said_plainly(m);
        assert!(
            said("out-of-spec: InvalidFooter")
                .unwrap()
                .contains("(invalid footer)")
        );
        assert!(said("OutOfSpec").unwrap().contains("--format"));
        assert_eq!(
            said("InvalidUtf8 at character 0").unwrap(),
            "The file is not UTF-8 text at character 0."
        );
        assert_eq!(said("out-of-spec: the footer is short"), None);
        assert_eq!(said("bad header"), None);
    }

    /// An expansion that found nothing says so instead of printing Polars' input.
    #[test]
    fn a_pattern_that_matched_nothing_says_so() {
        let csv = "failed to retrieve file schemas (csv): expanded paths were empty \
                   (path expansion input: 'paths: [PlRefPath { inner: \"/d/x?.csv\" }]', \
                   glob: true).";
        let err = PolarsError::ComputeError(csv.into());
        assert_eq!(
            user_message_from_polars(&err),
            "No files match this pattern."
        );
        let dir = "failed to retrieve first file schema (parquet): expanded paths were \
                   empty (path expansion input: 'paths: [PlRefPath { inner: \"/d/empty\" }]', \
                   glob: true). Hint: passing a schema can allow this scan to succeed.";
        let err = PolarsError::ComputeError(dir.into());
        assert_eq!(user_message_from_polars(&err), "No files found there.");
    }

    /// A temp copy is named by what the user opened, wherever the message says it,
    /// and a message that does not mention it is left alone.
    #[test]
    fn a_temp_copy_is_named_by_its_source() {
        let file = Path::new("/home/u/tmp/.tmp9tY5X2.parquet");
        let url = Path::new("http://host/broken.parquet");
        let said = named_by_source(
            "\"/home/u/tmp/.tmp9tY5X2.parquet\": Bad.\nIt stopped at /home/u/tmp/.tmp9tY5X2.parquet.",
            file,
            url,
        );
        assert_eq!(
            said,
            "\"http://host/broken.parquet\": Bad.\nIt stopped at http://host/broken.parquet."
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
            let message = format!("\"{other}\": Bad.");
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
            said.contains("a column named \"39\" where another has \"25\""),
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

    /// A file a spreadsheet app holds is named, with what to do, whichever way the
    /// sharing violation arrives: from datui's own open, or rewrapped by Polars with
    /// the path in its text.
    #[test]
    fn a_file_another_program_holds_says_so() {
        let path = Path::new(r"C:\data\book.xlsx");
        let held = format!(
            "\"{}\": The file is open in another program that does not allow reading it; \
             close it there and reopen.",
            path.display()
        );
        let raw = || io::Error::from_raw_os_error(32);
        let rewrapped = || {
            io::Error::other(format!(
                "The process cannot access the file because it is being used by another \
                 process. (os error 32): {}",
                path.display()
            ))
        };
        for report in [
            color_eyre::eyre::Report::new(raw()),
            color_eyre::eyre::Report::new(PolarsError::from(rewrapped())),
            color_eyre::eyre::Report::new(PolarsError::from(raw()).context("scan".into())),
            color_eyre::eyre::eyre!("{}", PolarsError::from(rewrapped())),
        ] {
            assert_eq!(
                report_message(true, &report, Some(path)),
                held,
                "{report:?}"
            );
            // Off Windows the codes mean something else. (On Windows the io message
            // inside says so whatever `report_message` is told.)
            if !cfg!(windows) {
                let elsewhere = report_message(false, &report, Some(path));
                assert!(!elsewhere.contains("another program"), "{elsewhere}");
            }
        }
        assert!(held_on(true, &raw()));
        assert!(held_on(true, &io::Error::from_raw_os_error(33)));
        assert!(!held_on(true, &io::Error::from_raw_os_error(5)));
        // EPIPE off Windows.
        assert!(!held_on(false, &raw()));
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
            msg.contains("column \"column\""),
            "expected offending column in message: {}",
            msg
        );
        assert!(msg.contains("--infer-rows"), "expected CLI hint: {}", msg);
        assert!(msg.contains("--null"), "expected null-value hint: {}", msg);
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
