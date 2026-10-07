//! User-facing error messages, matched on types (PolarsError variants, io::ErrorKind)
//! rather than strings.

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
    // Polars' words, then tidying every message needs: the query plan cut off, and a union
    // schema clash said as a directory problem. Around the match, so no arm skips it.
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

/// A scan whose path expanded to no files, said plainly instead of Polars' internals. A
/// local path is a pattern only when no file has its name (`source::expands_as_glob`).
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
    /// What a SQL writer can act on: the column, how many values, a few, and SQL to get past
    /// them. `rows` is `df`'s height if known; "N of M" only when Polars checked all,
    /// otherwise a lower bound from the batch it stopped in.
    pub fn sql_message(&self, rows: Option<usize>) -> String {
        let column = crate::query::sql_assist::sql_name(&self.column);
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

/// Whether `err` is Windows refusing a file another program holds: a sharing violation
/// (32, e.g. a spreadsheet open) or locked region (33). Polars rewraps opens keeping
/// the OS text but not the code, so the text is read too. Other OSes: never.
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

/// `message` with `file` (a temp copy datui made: a download, a decompressed CSV)
/// renamed `source`, what the user opened. Only whole mentions are replaced (no path
/// character on either side, though a sentence's full stop may follow), so longer paths
/// sharing the name are untouched.
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

/// Polars' words without the appended query plan (`Resolved plan until failure:` and
/// the `FAILED HERE` fragment), keeping only the file the scan stopped at, which the
/// plan names and the message often does not.
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

/// The path in the failing plan node's scan (`Csv SCAN [/data/one.csv]`): the one under
/// the marker, not the plan's first scan, which in a union or join is often not the
/// failing file.
fn file_in_plan(plan: &str) -> Option<&str> {
    const MARKER: &str = "FAILED HERE";
    let from = plan.find(MARKER).map_or(0, |at| at + MARKER.len());
    let at = plan[from..].find(" SCAN [")? + from + " SCAN [".len();
    let rest = &plan[at..];
    let end = rest.find(']')?;
    let file = rest[..end].trim();
    (!file.is_empty()).then_some(file)
}

/// Files that could not be stacked into one table, said as a directory rather than two
/// unlabeled full schemas. The reader already unions by name and widens types, so
/// what remains needs which file and what to do.
fn is_union_schema_error(msg: &str) -> bool {
    // Only a multi-file read's phrase: `unable to vstack` also comes from single-file
    // buffer stitching and DQ segments, where advising "open one file" would hide the
    // cause.
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

/// Files whose columns could not be lined up, said as files. Polars' `schema names
/// differ: got 39, expected 25` counts names, often a headerless file's first row read
/// as names.
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
mod tests;
