//! User-facing error message formatting.
//!
//! Uses typed error matching (PolarsError variants, io::ErrorKind) rather than
//! string parsing to produce actionable, implementation-agnostic messages.

use polars::prelude::PolarsError;
use std::io;
use std::path::Path;

/// Format a PolarsError as a user-facing message by matching on its variant.
pub fn user_message_from_polars(err: &PolarsError) -> String {
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

/// Light cleanup for ComputeError messages: strip Polars-internal phrasing.
fn simplify_compute_message(msg: &str) -> String {
    crate::query::sanitize_query_error(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
