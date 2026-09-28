//! The system clipboard, reached two ways, and the shapes a copy takes.
//!
//! **native** talks to the display server through arboard. On Wayland the copy
//! is owned by this process, so it survives only as long as datui runs unless a
//! clipboard manager persists it. For tabular copies the native path offers two
//! flavors at once — `text/html` (a real `<table>`) beside plain text — so a
//! paste into a spreadsheet or an email lands as a table while a paste into a
//! terminal stays TSV.
//!
//! **osc52** prints an `OSC 52` escape sequence for the terminal to act on,
//! which is what works over SSH with no display server in sight. The sequence
//! must go straight to stdout: the ratatui buffer is sanitized
//! ([`crate::sanitize`]), and an escape drawn as cell text is an escape
//! stripped. Terminals cap how much OSC 52 they accept, so the payload is
//! capped here first, with the limit in the config where a generous terminal's
//! user can raise it.
//!
//! **auto** is native where it initializes and osc52 everywhere else, decided
//! once per run at the first copy.

use polars::prelude::*;
use std::io::Write as _;

/// Which clipboard mechanism `[clipboard] backend` asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackendChoice {
    #[default]
    Auto,
    Native,
    Osc52,
}

impl BackendChoice {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "native" => Some(Self::Native),
            "osc52" => Some(Self::Osc52),
            _ => None,
        }
    }
}

/// One copy, ready for whichever destination takes it. The text flavor is
/// always there; the HTML flavor rides along when the copy is a table and the
/// destination can offer both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Payload {
    pub text: String,
    pub html: Option<String>,
}

impl Payload {
    pub fn text(text: String) -> Self {
        Self { text, html: None }
    }
}

/// Somewhere a payload can go. A trait so the integration tests can hand the
/// app a destination that only records what it was given.
pub trait Destination {
    fn write(&mut self, payload: &Payload) -> Result<(), String>;
    /// One word for the flash and for errors: "clipboard" or "terminal".
    fn describe(&self) -> &'static str;
}

/// arboard, kept alive for the life of the app: on Wayland and X11 the copy
/// dies with the process that owns it, so dropping the handle early would
/// revoke the copy the flash just announced.
pub struct Native {
    clipboard: arboard::Clipboard,
}

impl Native {
    pub fn new() -> Result<Self, String> {
        arboard::Clipboard::new()
            .map(|clipboard| Self { clipboard })
            .map_err(|e| format!("clipboard unavailable: {e}"))
    }
}

impl Destination for Native {
    fn write(&mut self, payload: &Payload) -> Result<(), String> {
        let result = match &payload.html {
            Some(html) => self
                .clipboard
                .set_html(html.clone(), Some(payload.text.clone())),
            None => self.clipboard.set_text(payload.text.clone()),
        };
        result.map_err(|e| format!("copy failed: {e}"))
    }

    fn describe(&self) -> &'static str {
        "clipboard"
    }
}

/// The `OSC 52` writer. Carries only the cap; stdout is fetched per write.
pub struct Osc52 {
    /// Longest base64 payload to attempt, in bytes.
    pub limit: usize,
}

impl Destination for Osc52 {
    fn write(&mut self, payload: &Payload) -> Result<(), String> {
        let sequence = osc52_sequence(&payload.text, self.limit)?;
        let mut out = std::io::stdout();
        out.write_all(sequence.as_bytes())
            .and_then(|()| out.flush())
            .map_err(|e| format!("copy failed: {e}"))
    }

    fn describe(&self) -> &'static str {
        "terminal"
    }
}

/// The escape sequence that asks the terminal to set the system clipboard,
/// or why it was not built. Split from [`Osc52::write`] so a test can read
/// the bytes without owning stdout.
pub fn osc52_sequence(text: &str, limit: usize) -> Result<String, String> {
    use base64::Engine as _;
    let encoded = base64::engine::general_purpose::STANDARD.encode(text.as_bytes());
    if encoded.len() > limit {
        return Err(format!(
            "the copy is {} of base64 and the terminal path is capped at {} \
             (raise [clipboard] osc52_limit_kb, or export to a file)",
            format_kb(encoded.len()),
            format_kb(limit),
        ));
    }
    Ok(format!("\x1b]52;c;{encoded}\x07"))
}

fn format_kb(bytes: usize) -> String {
    format!("{} KB", bytes.div_ceil(1024))
}

/// The destination a backend choice names, built at the first copy.
pub fn destination(
    choice: BackendChoice,
    osc52_limit: usize,
) -> Result<Box<dyn Destination>, String> {
    match choice {
        BackendChoice::Native => Native::new().map(|n| Box::new(n) as Box<dyn Destination>),
        BackendChoice::Osc52 => Ok(Box::new(Osc52 { limit: osc52_limit })),
        BackendChoice::Auto => Ok(match Native::new() {
            Ok(native) => Box::new(native),
            // No display server to talk to — an SSH session — is exactly
            // what the escape-sequence path is for.
            Err(_) => Box::new(Osc52 { limit: osc52_limit }),
        }),
    }
}

// ----- The shapes a copy takes -----

/// The formats the dialog offers for tabular scopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CopyFormat {
    #[default]
    Tsv,
    Csv,
    Markdown,
}

impl CopyFormat {
    pub const ALL: [Self; 3] = [Self::Tsv, Self::Csv, Self::Markdown];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tsv => "TSV",
            Self::Csv => "CSV",
            Self::Markdown => "Markdown",
        }
    }
}

/// A DataFrame as delimited text, quoted the way spreadsheets parse a paste:
/// only fields holding the delimiter, a quote or a newline are quoted, which
/// is `QuoteStyle::Necessary`, the writer's default. Raw values, like export;
/// a null is an empty field, never the UI's `∅`.
pub fn delimited(df: &DataFrame, separator: u8, header: bool) -> Result<String, String> {
    let mut out = Vec::new();
    let mut df = df.clone();
    CsvWriter::new(&mut out)
        .with_separator(separator)
        .include_header(header)
        .finish(&mut df)
        .map_err(|e| e.to_string())?;
    let mut text = String::from_utf8(out).map_err(|e| e.to_string())?;
    // The writer ends the last record with a newline; a paste target treats
    // that as an empty extra row.
    while text.ends_with('\n') || text.ends_with('\r') {
        text.pop();
    }
    Ok(text)
}

/// A DataFrame as a Markdown table. Always with the header: the delimiter row
/// under it is what makes Markdown read the block as a table at all. Numeric
/// columns declare right alignment, pipes are escaped, and embedded newlines
/// flatten to spaces — a Markdown cell has no way to hold one.
pub fn markdown(df: &DataFrame) -> Result<String, String> {
    let column_names = df.get_column_names_owned();
    let escape = |s: &str| s.replace('|', "\\|").replace(['\n', '\r'], " ");
    let mut names: Vec<String> = Vec::with_capacity(column_names.len());
    let mut cells: Vec<Vec<String>> = Vec::with_capacity(column_names.len());
    let mut numeric: Vec<bool> = Vec::with_capacity(column_names.len());
    for name in &column_names {
        let column = df.column(name).map_err(|e| e.to_string())?;
        names.push(escape(name));
        numeric.push(column.dtype().is_primitive_numeric());
        let series = column.as_materialized_series();
        let mut body = Vec::with_capacity(df.height());
        for i in 0..df.height() {
            let value = series.get(i).map_err(|e| e.to_string())?;
            body.push(match value {
                AnyValue::Null => String::new(),
                v => escape(&v.str_value()),
            });
        }
        cells.push(body);
    }
    let widths: Vec<usize> = names
        .iter()
        .zip(&cells)
        .map(|(name, body)| {
            body.iter()
                .map(|c| c.chars().count())
                .max()
                .unwrap_or(0)
                .max(name.chars().count())
                .max(3)
        })
        .collect();
    // A numeric column is padded to the right, so the raw text reads the way
    // the `---:` delimiter tells a renderer to draw it; the two agree.
    let pad = |s: &str, w: usize, right: bool| {
        let fill = " ".repeat(w - s.chars().count());
        if right {
            format!("{fill}{s}")
        } else {
            format!("{s}{fill}")
        }
    };
    let mut lines = Vec::with_capacity(df.height() + 2);
    lines.push(format!(
        "| {} |",
        names
            .iter()
            .zip(&widths)
            .zip(&numeric)
            .map(|((n, &w), &num)| pad(n, w, num))
            .collect::<Vec<_>>()
            .join(" | ")
    ));
    lines.push(format!(
        "|{}|",
        widths
            .iter()
            .zip(&numeric)
            .map(|(&w, &num)| {
                if num {
                    format!(" {}: ", "-".repeat(w.saturating_sub(1)))
                } else {
                    format!(" {} ", "-".repeat(w))
                }
            })
            .collect::<Vec<_>>()
            .join("|")
    ));
    for row in 0..df.height() {
        lines.push(format!(
            "| {} |",
            cells
                .iter()
                .zip(&widths)
                .zip(&numeric)
                .map(|((body, &w), &num)| pad(&body[row], w, num))
                .collect::<Vec<_>>()
                .join(" | ")
        ));
    }
    Ok(lines.join("\n"))
}

/// A DataFrame as an HTML table, the rich flavor beside a TSV or CSV copy.
/// Everything is escaped; a null is an empty cell.
pub fn html_table(df: &DataFrame, header: bool) -> Result<String, String> {
    let escape = |s: &str| {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    };
    let column_names = df.get_column_names_owned();
    let mut out = String::from("<table>");
    if header {
        out.push_str("<thead><tr>");
        for name in &column_names {
            out.push_str(&format!("<th>{}</th>", escape(name)));
        }
        out.push_str("</tr></thead>");
    }
    out.push_str("<tbody>");
    for row in 0..df.height() {
        out.push_str("<tr>");
        for name in &column_names {
            let value = df
                .column(name)
                .map_err(|e| e.to_string())?
                .as_materialized_series()
                .get(row)
                .map_err(|e| e.to_string())?;
            let text = match value {
                AnyValue::Null => String::new(),
                v => v.str_value().into_owned(),
            };
            out.push_str(&format!("<td>{}</td>", escape(&text)));
        }
        out.push_str("</tr>");
    }
    out.push_str("</tbody></table>");
    Ok(out)
}

/// The payload for a tabular copy: the chosen format as text, with the HTML
/// flavor beside a TSV or CSV copy. A Markdown copy is the Markdown itself —
/// pasting rich HTML where Markdown was asked for would defeat the choice.
pub fn tabular_payload(
    df: &DataFrame,
    format: CopyFormat,
    header: bool,
) -> Result<Payload, String> {
    let text = match format {
        CopyFormat::Tsv => delimited(df, b'\t', header)?,
        CopyFormat::Csv => delimited(df, b',', header)?,
        CopyFormat::Markdown => markdown(df)?,
    };
    let html = match format {
        CopyFormat::Tsv | CopyFormat::Csv => Some(html_table(df, header)?),
        CopyFormat::Markdown => None,
    };
    Ok(Payload { text, html })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tricky() -> DataFrame {
        df!(
            "name" => ["plain", "tab\there", "pipe|and\nnewline", "\"quoted\""],
            "n" => [Some(1i64), Some(2), None, Some(4)],
        )
        .unwrap()
    }

    #[test]
    fn tsv_quotes_only_what_a_paste_needs_quoted() {
        let text = delimited(&tricky(), b'\t', true).unwrap();
        let lines: Vec<&str> = text.split('\n').collect();
        assert_eq!(lines[0], "name\tn");
        assert_eq!(lines[1], "plain\t1");
        // The embedded tab and newline are quoted, so a spreadsheet reads one cell.
        assert_eq!(lines[2], "\"tab\there\"\t2");
        assert!(text.contains("\"pipe|and\nnewline\"\t"));
        // A null is an empty field, and nothing here is the UI's null glyph.
        assert!(!text.contains('∅'));
        assert!(text.ends_with("\"\"\"quoted\"\"\"\t4"), "{text:?}");
    }

    #[test]
    fn header_toggle_is_honored() {
        let with = delimited(&tricky(), b',', true).unwrap();
        let without = delimited(&tricky(), b',', false).unwrap();
        assert!(with.starts_with("name,n"));
        assert!(without.starts_with("plain,1"));
    }

    #[test]
    fn markdown_escapes_aligns_and_keeps_nulls_empty() {
        let text = markdown(&tricky()).unwrap();
        let lines: Vec<&str> = text.split('\n').collect();
        assert!(lines[0].starts_with("| name"));
        // The numeric column's delimiter declares right alignment, and the raw
        // text pads its header and cells the same way, so the two agree.
        assert!(lines[1].contains("-: |"), "{}", lines[1]);
        assert!(lines[0].ends_with("|   n |"), "{}", lines[0]);
        assert!(lines[2].ends_with("|   1 |"), "{}", lines[2]);
        assert!(text.contains("pipe\\|and newline"), "{text}");
        // Every row spans the same padded width.
        let width = lines[0].chars().count();
        assert!(lines.iter().all(|l| l.chars().count() == width), "{text}");
    }

    #[test]
    fn html_flavor_escapes_and_rides_beside_tsv_only() {
        let payload = tabular_payload(&tricky(), CopyFormat::Tsv, true).unwrap();
        let html = payload.html.expect("tsv carries the html flavor");
        assert!(html.starts_with("<table><thead>"));
        assert!(html.contains("<td>\"quoted\"</td>"));
        let md = tabular_payload(&tricky(), CopyFormat::Markdown, true).unwrap();
        assert!(md.html.is_none(), "markdown is its own rich flavor");
    }

    #[test]
    fn html_escapes_markup_in_values() {
        let df = df!("x" => ["<b>&"]).unwrap();
        let html = html_table(&df, false).unwrap();
        assert!(html.contains("<td>&lt;b&gt;&amp;</td>"), "{html}");
    }

    #[test]
    fn osc52_wraps_base64_and_the_cap_names_the_config() {
        let seq = osc52_sequence("hello", 1024).unwrap();
        assert_eq!(seq, "\x1b]52;c;aGVsbG8=\x07");
        let err = osc52_sequence("hello world, far too long", 8).unwrap_err();
        assert!(err.contains("osc52_limit_kb"), "{err}");
    }

    #[test]
    fn backend_choice_parses_the_config_words() {
        assert_eq!(BackendChoice::parse("auto"), Some(BackendChoice::Auto));
        assert_eq!(BackendChoice::parse("Native"), Some(BackendChoice::Native));
        assert_eq!(BackendChoice::parse("OSC52"), Some(BackendChoice::Osc52));
        assert_eq!(BackendChoice::parse("wayland"), None);
    }
}
