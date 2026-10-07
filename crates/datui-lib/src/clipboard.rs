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
//! user can raise it. The cap is known before a copy is built: a table copy to
//! the terminal is read in batches and stops at the first byte over it, and no
//! HTML flavor is built for a destination that cannot offer one.
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
    /// Takes the payload: a destination that keeps it owns it, without a copy.
    fn write(&mut self, payload: Payload) -> Result<(), String>;
    /// One word for the flash and for errors: "clipboard" or "terminal".
    fn describe(&self) -> &'static str;
    fn accepts(&self) -> Accepts {
        Accepts {
            html: true,
            base64_limit: None,
        }
    }
}

/// What a destination takes, known before a copy is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Accepts {
    /// Offers an HTML flavor beside the text.
    pub html: bool,
    /// The longest copy it takes, in bytes of base64; none for no cap.
    pub base64_limit: Option<usize>,
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
    fn write(&mut self, payload: Payload) -> Result<(), String> {
        let result = match payload.html {
            Some(html) => self.clipboard.set_html(html, Some(payload.text)),
            None => self.clipboard.set_text(payload.text),
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
    fn write(&mut self, payload: Payload) -> Result<(), String> {
        let sequence = osc52_sequence(&payload.text, self.limit)?;
        // Encoded, the text is not needed while the sequence is written.
        drop(payload);
        let mut out = std::io::stdout();
        out.write_all(sequence.as_bytes())
            .and_then(|()| out.flush())
            .map_err(|e| format!("copy failed: {e}"))
    }

    fn describe(&self) -> &'static str {
        "terminal"
    }

    fn accepts(&self) -> Accepts {
        Accepts {
            html: false,
            base64_limit: Some(self.limit),
        }
    }
}

/// The length of `bytes` bytes in padded base64.
pub fn base64_len(bytes: usize) -> usize {
    bytes.div_ceil(3).saturating_mul(4)
}

/// The escape sequence that asks the terminal to set the system clipboard,
/// or why it was not built. Split from [`Osc52::write`] so a test can read
/// the bytes without owning stdout. The size is checked before anything is
/// encoded.
pub fn osc52_sequence(text: &str, limit: usize) -> Result<String, String> {
    use base64::Engine as _;
    let encoded = base64_len(text.len());
    if encoded > limit {
        return Err(over_osc52_limit(Some(encoded), limit));
    }
    let mut sequence = String::with_capacity(encoded + 8);
    sequence.push_str("\x1b]52;c;");
    base64::engine::general_purpose::STANDARD.encode_string(text.as_bytes(), &mut sequence);
    sequence.push('\x07');
    Ok(sequence)
}

/// Why a copy does not go through the terminal. `encoded` is its size in base64,
/// or none when it stopped being built at the cap.
pub(crate) fn over_osc52_limit(encoded: Option<usize>, limit: usize) -> String {
    let size = match encoded {
        Some(bytes) => format_kb(bytes),
        None => format!("over {}", format_kb(limit)),
    };
    format!(
        "the copy is {size} of base64 and the terminal path is capped at {} \
         (raise [clipboard] osc52_limit, or export to a file)",
        format_kb(limit),
    )
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
    let mut df = crate::nested_json::frame_as_json(df).map_err(|e| e.to_string())?;
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
    let mut layout = MarkdownLayout::new(df);
    layout.measure(df)?;
    let mut out = String::with_capacity(layout.len(df.height()));
    layout.write(df, &mut out)?;
    Ok(out)
}

/// A Markdown table's columns: their names, alignment and widths. Widths are
/// measured over every row before a row is written, a cell at a time, so no
/// cell's text is held beyond its own.
struct MarkdownLayout {
    names: Vec<String>,
    numeric: Vec<bool>,
    widths: Vec<usize>,
}

impl MarkdownLayout {
    fn new(df: &DataFrame) -> Self {
        let names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|name| markdown_escape(name))
            .collect();
        let widths = names.iter().map(|n| n.chars().count().max(3)).collect();
        let numeric = df
            .columns()
            .iter()
            .map(|c| c.dtype().is_primitive_numeric())
            .collect();
        Self {
            names,
            numeric,
            widths,
        }
    }

    /// Widen the columns to fit the rows of `df`.
    fn measure(&mut self, df: &DataFrame) -> Result<(), String> {
        for (column, width) in df.columns().iter().zip(&mut self.widths) {
            let series = column.as_materialized_series();
            for row in 0..df.height() {
                *width = (*width).max(markdown_cell(series, row)?.chars().count());
            }
        }
        Ok(())
    }

    /// The table's length over `rows` rows at the widths measured so far, in
    /// characters: no more than its bytes, and only ever growing as rows widen
    /// the columns.
    fn len(&self, rows: usize) -> usize {
        let line = self.widths.iter().sum::<usize>() + 3 * self.widths.len() + 1;
        (rows + 2) * line + rows + 1
    }

    fn write_line<'a>(&self, out: &mut String, cells: impl Iterator<Item = &'a str>) {
        out.push_str("| ");
        for (i, ((cell, &width), &right)) in cells.zip(&self.widths).zip(&self.numeric).enumerate()
        {
            if i > 0 {
                out.push_str(" | ");
            }
            // A numeric column is padded to the right, so the raw text reads the
            // way the `---:` delimiter tells a renderer to draw it; the two agree.
            let fill = width - cell.chars().count();
            if right {
                out.extend(std::iter::repeat_n(' ', fill));
                out.push_str(cell);
            } else {
                out.push_str(cell);
                out.extend(std::iter::repeat_n(' ', fill));
            }
        }
        out.push_str(" |");
    }

    /// Append the header, the delimiter row and the rows of `df`.
    fn write(&self, df: &DataFrame, out: &mut String) -> Result<(), String> {
        self.write_line(out, self.names.iter().map(String::as_str));
        out.push_str("\n|");
        for (i, (&width, &right)) in self.widths.iter().zip(&self.numeric).enumerate() {
            if i > 0 {
                out.push('|');
            }
            out.push(' ');
            if right {
                out.extend(std::iter::repeat_n('-', width.saturating_sub(1)));
                out.push(':');
            } else {
                out.extend(std::iter::repeat_n('-', width));
            }
            out.push(' ');
        }
        out.push('|');
        self.write_rows(df, out)
    }

    /// Append the rows of `df`, each on a line of its own.
    fn write_rows(&self, df: &DataFrame, out: &mut String) -> Result<(), String> {
        let series: Vec<&Series> = df
            .columns()
            .iter()
            .map(Column::as_materialized_series)
            .collect();
        let mut cells = Vec::with_capacity(series.len());
        for row in 0..df.height() {
            cells.clear();
            for s in &series {
                cells.push(markdown_cell(s, row)?);
            }
            out.push('\n');
            self.write_line(out, cells.iter().map(String::as_str));
        }
        Ok(())
    }
}

fn markdown_escape(s: &str) -> String {
    s.replace('|', "\\|").replace(['\n', '\r'], " ")
}

/// One Markdown cell: the value escaped, a null empty.
fn markdown_cell(series: &Series, row: usize) -> Result<String, String> {
    Ok(match series.get(row).map_err(|e| e.to_string())? {
        AnyValue::Null => String::new(),
        // Exact, as the TSV and CSV writers are: Polars' own display would
        // round a float to its compact preview.
        v => markdown_escape(&crate::exact::value_text(&v)),
    })
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
                v => crate::exact::value_text(&v),
            };
            out.push_str(&format!("<td>{}</td>", escape(&text)));
        }
        out.push_str("</tr>");
    }
    out.push_str("</tbody></table>");
    Ok(out)
}

/// The payload for a tabular copy: the chosen format as text, with the HTML
/// flavor beside a TSV or CSV copy when `html` asks for it (the destination can
/// offer it). A Markdown copy is the Markdown itself — pasting rich HTML where
/// Markdown was asked for would defeat the choice. List and struct cells are
/// JSON in every format, as in a CSV export.
pub fn tabular_payload(
    df: &DataFrame,
    format: CopyFormat,
    header: bool,
    html: bool,
) -> Result<Payload, String> {
    let df = &crate::nested_json::frame_as_cells(df).map_err(|e| e.to_string())?;
    let text = match format {
        CopyFormat::Tsv => delimited(df, b'\t', header)?,
        CopyFormat::Csv => delimited(df, b',', header)?,
        CopyFormat::Markdown => markdown(df)?,
    };
    let html = match format {
        CopyFormat::Tsv | CopyFormat::Csv if html => Some(html_table(df, header)?),
        _ => None,
    };
    Ok(Payload { text, html })
}

/// Rows a table copy to a capped destination is read in: what runs past the cap
/// is at most this many rows.
const BOUNDED_BATCH_ROWS: usize = 1024;

/// The text of a table copy and its row count, read from `lf` a batch at a time
/// and given up at the first batch that takes it past `limit` bytes of base64:
/// a copy the terminal will not take is never read or written whole. Text only;
/// a capped destination offers no HTML flavor.
///
/// What the query does upstream of its last rows (a sort, a join) still takes
/// its own memory; a Markdown copy keeps its rows until the widths are known,
/// which the cap bounds as it does the text.
pub fn bounded_table_text(
    lf: LazyFrame,
    format: CopyFormat,
    header: bool,
    limit: usize,
) -> Result<(String, usize), String> {
    use std::sync::{Arc, Mutex};
    let polars_error = |e: PolarsError| crate::error_display::user_message_from_polars(&e);
    let schema = lf.clone().collect_schema().map_err(polars_error)?;
    let state = Arc::new(Mutex::new(BoundedText::new(format, header, limit)));
    let sink_state = Arc::clone(&state);
    let sink = lf
        .sink_batches(
            PlanCallback::new(move |batch: DataFrame| {
                let mut text = sink_state
                    .lock()
                    .map_err(|_| PolarsError::ComputeError("copy lock failed".into()))?;
                // True stops the read: the copy is over the cap, or failed.
                Ok(text.take(batch))
            }),
            true,
            std::num::NonZeroUsize::new(BOUNDED_BATCH_ROWS),
        )
        .map_err(polars_error)?;
    // Streaming whatever the setting: the in-memory engine collects the whole result
    // before the first batch, which is what stopping at the cap is here to avoid.
    crate::statistics::collect_lazy(sink, true).map_err(polars_error)?;
    let mut text = std::mem::replace(
        &mut *state.lock().map_err(|_| "copy lock failed".to_string())?,
        BoundedText::new(format, header, limit),
    );
    if !text.started {
        // No batch came: the header alone, from the schema.
        text.take(DataFrame::empty_with_schema(&schema));
    }
    text.finish()
}

/// A table copy's text as its batches come in, against the cap.
struct BoundedText {
    format: CopyFormat,
    header: bool,
    limit: usize,
    started: bool,
    text: String,
    rows: usize,
    /// A Markdown copy's columns and the rows to write once they are measured.
    markdown: Option<(MarkdownLayout, Vec<DataFrame>)>,
    over: bool,
    error: Option<String>,
}

impl BoundedText {
    fn new(format: CopyFormat, header: bool, limit: usize) -> Self {
        Self {
            format,
            header,
            limit,
            started: false,
            text: String::new(),
            rows: 0,
            markdown: None,
            over: false,
            error: None,
        }
    }

    /// Add a batch; true once the copy cannot go on.
    fn take(&mut self, batch: DataFrame) -> bool {
        if self.over || self.error.is_some() {
            return true;
        }
        if let Err(e) = self.try_take(batch) {
            self.error = Some(e);
        }
        self.over || self.error.is_some()
    }

    fn try_take(&mut self, batch: DataFrame) -> Result<(), String> {
        let first = !self.started;
        self.started = true;
        self.rows += batch.height();
        let batch = crate::nested_json::frame_as_cells(&batch).map_err(|e| e.to_string())?;
        let separator = match self.format {
            CopyFormat::Tsv => b'\t',
            CopyFormat::Csv => b',',
            CopyFormat::Markdown => {
                let (layout, rows) = self
                    .markdown
                    .get_or_insert_with(|| (MarkdownLayout::new(&batch), Vec::new()));
                layout.measure(&batch)?;
                // The widths so far are a floor on the final ones.
                self.over = base64_len(layout.len(self.rows)) > self.limit;
                rows.push(batch);
                return Ok(());
            }
        };
        let mut out = Vec::new();
        let mut batch = crate::nested_json::frame_as_json(&batch).map_err(|e| e.to_string())?;
        CsvWriter::new(&mut out)
            .with_separator(separator)
            .include_header(first && self.header)
            .finish(&mut batch)
            .map_err(|e| e.to_string())?;
        self.text
            .push_str(&String::from_utf8(out).map_err(|e| e.to_string())?);
        // The last record's newline is trimmed at the end.
        self.over = base64_len(self.text.len().saturating_sub(1)) > self.limit;
        Ok(())
    }

    fn finish(mut self) -> Result<(String, usize), String> {
        if let Some(e) = self.error {
            return Err(e);
        }
        if self.over {
            return Err(over_osc52_limit(None, self.limit));
        }
        if let Some((layout, frames)) = self.markdown.take() {
            self.text.reserve(layout.len(self.rows));
            let mut frames = frames.iter();
            if let Some(first) = frames.next() {
                layout.write(first, &mut self.text)?;
            }
            for frame in frames {
                layout.write_rows(frame, &mut self.text)?;
            }
        }
        while self.text.ends_with('\n') || self.text.ends_with('\r') {
            self.text.pop();
        }
        let encoded = base64_len(self.text.len());
        if encoded > self.limit {
            return Err(over_osc52_limit(Some(encoded), self.limit));
        }
        Ok((self.text, self.rows))
    }
}

#[cfg(test)]
mod tests;
