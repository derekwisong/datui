//! Info panel: tabbed Schema and Resources view for dataset technical info.

use std::collections::HashMap;

use crate::numfmt::group_chrome;
use std::path::Path;
use std::sync::Arc;

use polars::prelude::*;

use crate::formats::parquet_footer::Footer;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::Stylize;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{HighlightSpacing, Paragraph, Row, StatefulWidget, Table, Widget};

use crate::export_modal::ExportFormat;
use crate::render::context::RenderContext;
use crate::table::DataTableState;
use crate::widgets::ui::{HintBar, SectionRule, Surface};

/// One drawn line of the Notes tab.
struct NoteRow {
    text: String,
    /// Drawn in the panel's dim color: the line a note rests on, not the note.
    dim: bool,
}

/// Which notes to draw, as a half-open range. `heights` are each note's rows (a blank
/// line between notes), `stored` the last scroll, `show` the rows available.
/// Guarantees:
///
/// - `first <= selected < last`: the cursor's note is always drawn;
/// - the range fits `show`, unless it is one note too tall alone (drawn as far as it
///   goes, else unreachable);
/// - `last` is as large as possible, never leaving rows blank while a whole note is
///   hidden.
fn notes_window(heights: &[usize], selected: usize, stored: usize, show: usize) -> (usize, usize) {
    if heights.is_empty() {
        return (0, 0);
    }
    let selected = selected.min(heights.len() - 1);
    // Rows that notes `a..b` take, counting the blank line between adjacent ones.
    let span = |a: usize, b: usize| heights[a..b].iter().sum::<usize>() + (b - a).saturating_sub(1);
    // Start no later than the selected note, and far enough back that it still fits.
    let mut first = stored.min(selected);
    while first < selected && span(first, selected + 1) > show {
        first += 1;
    }
    // Then take as many following notes as the rest of the panel holds.
    let mut last = selected + 1;
    while last < heights.len() && span(first, last + 1) <= show {
        last += 1;
    }
    // And give back any room left at the top, so the panel is never part empty while a
    // whole note is hidden above it.
    while first > 0 && span(first - 1, last) <= show {
        first -= 1;
    }
    (first, last)
}

/// A note's lines: its summary and the line saying what it is based on, both wrapped to
/// the panel. Selection changes only the marker, never a height.
fn note_rows(note: &crate::notes::Note, selected: bool, width: usize) -> Vec<NoteRow> {
    let mut rows = Vec::new();
    let marker = if selected {
        crate::glyphs::get().prompt
    } else {
        "  "
    };
    let mut first = true;
    for line in wrap_to(&note.summary, width.saturating_sub(2)) {
        rows.push(NoteRow {
            text: format!("{}{line}", if first { marker } else { "  " }),
            dim: false,
        });
        first = false;
    }
    for line in wrap_to(&note.scope, width.saturating_sub(4)) {
        rows.push(NoteRow {
            text: format!("    {line}"),
            dim: true,
        });
    }
    rows
}

/// Break `text` on spaces so no line passes `width` columns, measured in columns
/// (double-width names). A word longer than the panel stays whole (the terminal clips
/// it): wrapping protects the note's height.
pub(crate) fn wrap_to(text: &str, width: usize) -> Vec<String> {
    use unicode_width::UnicodeWidthStr;
    if width == 0 {
        return vec![text.to_string()];
    }
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in text.split_whitespace() {
        let room = if line.is_empty() {
            width
        } else {
            width.saturating_sub(line.width() + 1)
        };
        if word.width() > room && !line.is_empty() {
            lines.push(std::mem::take(&mut line));
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
    }
    if !line.is_empty() || lines.is_empty() {
        lines.push(line);
    }
    lines
}

/// `n` comma-grouped, for the panel's own labels.
pub(crate) fn group_u64(n: u64) -> String {
    let mut out = String::new();
    crate::numfmt::NumberFormat::CHROME.write_u64(n, &mut out);
    out
}

/// `n` and the noun for it: `1 tensor`, `291 tensors`.
pub(crate) fn count_of(n: u64, one: &str, many: &str) -> String {
    format!("{} {}", group_u64(n), if n == 1 { one } else { many })
}

/// A parameter count as a model card says it: `8.0B`, `124.4M`, `950`.
pub(crate) fn short_count(n: u64) -> String {
    const STEPS: [(u64, &str); 4] = [
        (1_000_000_000_000, "T"),
        (1_000_000_000, "B"),
        (1_000_000, "M"),
        (1_000, "K"),
    ];
    for (size, suffix) in STEPS {
        if n >= size {
            return format!("{:.1}{suffix}", n as f64 / size as f64);
        }
    }
    n.to_string()
}

/// Break one line into lines of at most `width` columns between words, dropping
/// break spaces and keeping leading indentation. Only a word wider than `width` is
/// broken, at the edge, so a long URL or hash shows whole.
fn wrap_words(text: &str, width: usize) -> Vec<String> {
    use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};
    let mut lines = Vec::new();
    let mut line = String::new();
    let mut line_width = 0;
    // A line a break started: spaces at its start are the break's, not indentation.
    let mut broken = false;
    let mut rest = text;
    while !rest.is_empty() {
        let space = rest.starts_with(' ');
        let end = rest
            .find(|c: char| (c == ' ') != space)
            .unwrap_or(rest.len());
        let (token, after) = rest.split_at(end);
        rest = after;
        let w = token.width();
        if space {
            if broken && line.is_empty() {
                continue;
            }
            if line_width + w <= width {
                line.push_str(token);
                line_width += w;
            } else {
                // Indentation wider than the room starts no line of its own.
                if !line.is_empty() {
                    lines.push(std::mem::take(&mut line));
                }
                line_width = 0;
                broken = true;
            }
            continue;
        }
        if line_width + w > width && !line.is_empty() && w <= width {
            lines.push(std::mem::take(&mut line).trim_end().to_string());
            line_width = 0;
            broken = true;
        }
        for c in token.chars() {
            let cw = c.width().unwrap_or(0);
            if line_width + cw > width && !line.is_empty() {
                lines.push(std::mem::take(&mut line));
                line_width = 0;
                broken = true;
            }
            line.push(c);
            line_width += cw;
        }
    }
    // Spaces dropped at a break leave no blank line after the text.
    if !(broken && line.is_empty()) || lines.is_empty() {
        lines.push(line);
    }
    lines
}

/// One metadata value as text: an array that was listed, or how long it is.
pub(crate) fn meta_text(value: &crate::formats::model_files::MetaValue) -> String {
    use crate::formats::model_files::MetaValue;
    match value {
        MetaValue::Text(text) => text.clone(),
        MetaValue::List { of, len, items } if items.len() as u64 == *len && *len > 0 => {
            let quote = *of == "strings";
            let items: Vec<String> = items
                .iter()
                .map(|i| if quote { format!("{i:?}") } else { i.clone() })
                .collect();
            format!("[{}]", items.join(", "))
        }
        MetaValue::List { of, len, .. } => format!("[{} {of}]", group_u64(*len)),
    }
}

/// The most of one metadata value a detail tab draws: a chat template shows whole, a
/// GGUF's embedded `tokenizer.json` (megabytes) would be rewrapped every frame.
pub(crate) const VALUE_SHOWN_BYTES: usize = 64 * 1024;

/// Metadata as drawn lines: the key on a value's first line, each value split at its
/// newlines and wrapped to the rest of `width`; past [`VALUE_SHOWN_BYTES`] a line says
/// how much more.
pub(crate) fn metadata_lines(
    metadata: &[(String, crate::formats::model_files::MetaValue)],
    width: usize,
) -> Vec<(String, String)> {
    use unicode_width::UnicodeWidthStr;
    let longest = metadata.iter().map(|(k, _)| k.width()).max().unwrap_or(0);
    // Two columns between key and value; the key takes no more than two fifths.
    let key_width = longest.min(width * 2 / 5).max(1);
    let value_width = width.saturating_sub(key_width + 2).max(1);
    let mut out = Vec::new();
    for (key, value) in metadata {
        let key_cell = format!(
            "{:<w$}  ",
            crate::glyphs::fit(key, key_width),
            w = key_width
        );
        let blank = " ".repeat(key_width + 2);
        // Borrowed, not copied: this runs every frame.
        let listed;
        let text = match value {
            crate::formats::model_files::MetaValue::Text(text) => text.as_str(),
            other => {
                listed = meta_text(other);
                listed.as_str()
            }
        };
        let cut = text.floor_char_boundary(VALUE_SHOWN_BYTES);
        let (text, more) = (&text[..cut], text.len() - cut);
        let mut first = true;
        for raw in text.split('\n') {
            // Tabs as a space and other control characters dropped, so the widths
            // measured here are the widths drawn.
            let clean: String = raw
                .chars()
                .filter_map(|c| match c {
                    '\t' => Some(' '),
                    c if c.is_control() => None,
                    c => Some(c),
                })
                .collect();
            for line in wrap_words(&clean, value_width) {
                let k = if first {
                    key_cell.clone()
                } else {
                    blank.clone()
                };
                first = false;
                out.push((k, line));
            }
        }
        if more > 0 {
            let g = crate::glyphs::get();
            out.push((
                blank,
                format!("{} {} more", g.ellipsis, crate::numfmt::bytes(more as u64)),
            ));
        }
    }
    out
}

/// A length of time as a clock: `0:03.250`, `1:02:03.250`.
pub(crate) fn clock(seconds: f64) -> String {
    let ms = (seconds.max(0.0) * 1000.0).round() as u64;
    let (h, m, s, ms) = (ms / 3_600_000, ms / 60_000 % 60, ms / 1000 % 60, ms % 1000);
    if h > 0 {
        format!("{h}:{m:02}:{s:02}.{ms:03}")
    } else {
        format!("{m}:{s:02}.{ms:03}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InfoTab {
    #[default]
    Schema,
    /// A delimited spec's metadata line, as key and value.
    Metadata,
    /// What the file says besides its rows ([`crate::formats::text_formats::Detail`]): a model's
    /// totals, a VCD header. Titled by the detail.
    Format,
    Resources,
    Partitions,
    Notes,
    /// What the catalog that lists the dataset says of it: the page `Ctrl+E` shows on
    /// home.
    Documentation,
}

/// Which of the optional tabs the dataset on screen offers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TabsOffered {
    /// A delimited spec read a metadata line.
    pub metadata: bool,
    /// The file said something besides its rows.
    pub format: bool,
    pub partitions: bool,
    pub notes: bool,
    /// A catalog lists the dataset. Not the state's to say: the App sets it.
    pub documentation: bool,
}

impl TabsOffered {
    /// What `state` offers; `facts_tab` is the format's tab the file facts fill, when
    /// they will (see [`InfoContext::facts_tab`]).
    pub fn of(state: &DataTableState, facts_tab: Option<&'static str>) -> Self {
        Self {
            metadata: state
                .delimited_read()
                .is_some_and(|read| read.metadata.is_some()),
            format: state.format_detail().is_some() || facts_tab.is_some(),
            partitions: state
                .partition_columns()
                .map(|v| !v.is_empty())
                .unwrap_or(false),
            notes: state.has_notes(),
            documentation: false,
        }
    }
}

impl InfoTab {
    /// The tabs on offer, in order: the file's own tab when it says something (beside the
    /// schema), Partitions for partitioned data, Notes when datui has notes.
    pub fn visible(offered: TabsOffered) -> Vec<InfoTab> {
        let mut tabs = vec![InfoTab::Schema];
        if offered.documentation {
            tabs.push(InfoTab::Documentation);
        }
        if offered.metadata {
            tabs.push(InfoTab::Metadata);
        }
        if offered.format {
            tabs.push(InfoTab::Format);
        }
        tabs.push(InfoTab::Resources);
        if offered.partitions {
            tabs.push(InfoTab::Partitions);
        }
        if offered.notes {
            tabs.push(InfoTab::Notes);
        }
        tabs
    }

    pub fn title(self) -> &'static str {
        match self {
            InfoTab::Schema => "Schema",
            InfoTab::Metadata => "Metadata",
            InfoTab::Format => "Format",
            InfoTab::Resources => "Resources",
            InfoTab::Partitions => "Partitions",
            InfoTab::Notes => "Notes",
            InfoTab::Documentation => "Documentation",
        }
    }

    /// Next tab, wrapping. A tab that is not on offer starts from the first.
    pub fn next(self, offered: TabsOffered) -> Self {
        let tabs = Self::visible(offered);
        let at = self.index(offered);
        tabs[(at + 1) % tabs.len()]
    }

    pub fn prev(self, offered: TabsOffered) -> Self {
        let tabs = Self::visible(offered);
        let at = self.index(offered);
        tabs[(at + tabs.len() - 1) % tabs.len()]
    }

    /// Where this tab sits among the ones on offer; 0 when it is not among them.
    pub fn index(self, offered: TabsOffered) -> usize {
        Self::visible(offered)
            .iter()
            .position(|tab| *tab == self)
            .unwrap_or(0)
    }
}

/// Modal state for the Info panel: focus, tab, schema table selection/scroll.
#[derive(Default)]
pub struct InfoModal {
    pub active_tab: InfoTab,
    pub schema_selected_index: usize,
    pub schema_scroll_offset: usize,
    pub schema_table_state: ratatui::widgets::TableState,
    /// Last visible height for schema table (data rows), set during render.
    pub schema_visible_height: usize,
    /// The note the cursor is on, and the first note drawn.
    pub notes_selected_index: usize,
    /// The first row of the notes list on screen; the render keeps the selected note
    /// inside the window.
    pub notes_scroll_offset: usize,
    /// The first line of a detail tab's list on screen. The render clamps it. One for
    /// all: a dataset has one detail tab at most.
    pub detail_scroll: usize,
    /// The list lines a detail tab last had room for; set during render.
    pub detail_visible: usize,
    /// The cursor in a detail tab listing the file's tables (worksheets, SQLite tables),
    /// where Enter opens one; the render clamps it and keeps it in view.
    pub detail_selected: usize,
}

impl InfoModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(&mut self) {
        self.open_on(InfoTab::Schema);
    }

    /// Open with `tab` in front. The accented `i` chip promises unread notes;
    /// arriving on the Schema tab instead made the reader hunt for them.
    pub fn open_on(&mut self, tab: InfoTab) {
        self.active_tab = tab;
        self.schema_selected_index = 0;
        self.schema_scroll_offset = 0;
        self.schema_table_state.select(Some(0));
        self.notes_selected_index = 0;
        self.notes_scroll_offset = 0;
        self.detail_scroll = 0;
        self.detail_selected = 0;
    }

    /// Switch to the next of the tabs on offer.
    pub fn switch_tab(&mut self, offered: TabsOffered) {
        self.active_tab = self.active_tab.next(offered);
        if self.active_tab == InfoTab::Schema {
            self.schema_selected_index = 0;
            self.schema_scroll_offset = 0;
            self.schema_table_state.select(Some(0));
        }
    }

    /// Switch to the previous of the tabs on offer.
    pub fn switch_tab_prev(&mut self, offered: TabsOffered) {
        self.active_tab = self.active_tab.prev(offered);
        if self.active_tab == InfoTab::Schema {
            self.schema_selected_index = 0;
            self.schema_scroll_offset = 0;
            self.schema_table_state.select(Some(0));
        }
    }

    /// Scroll a detail tab's list by `delta` lines; the render keeps it
    /// in range.
    pub fn detail_scroll_by(&mut self, delta: isize) {
        self.detail_scroll = self.detail_scroll.saturating_add_signed(delta);
    }

    /// Move the cursor through the notes; true if changed. Only the index moves: the render
    /// scrolls to the selection, so note heights never limit the cursor.
    pub fn notes_move(&mut self, delta: isize, total: usize) -> bool {
        if total == 0 {
            return false;
        }
        let last = total - 1;
        let next = (self.notes_selected_index as isize + delta).clamp(0, last as isize) as usize;
        if next == self.notes_selected_index {
            return false;
        }
        self.notes_selected_index = next;
        true
    }

    /// Scroll and selection for schema table. `total_rows` = schema len,
    /// `visible_height` = rows shown. Returns true if state changed.
    pub fn schema_table_down(&mut self, total_rows: usize, visible_height: usize) -> bool {
        if total_rows == 0 {
            return false;
        }
        let max_idx = total_rows.saturating_sub(1);
        if self.schema_selected_index >= max_idx {
            return false;
        }
        self.schema_selected_index += 1;
        let visible_end = self.schema_scroll_offset + visible_height;
        if visible_height > 0 && self.schema_selected_index >= visible_end {
            self.schema_scroll_offset = self.schema_selected_index + 1 - visible_height;
        }
        let local = self
            .schema_selected_index
            .saturating_sub(self.schema_scroll_offset);
        self.schema_table_state.select(Some(local));
        true
    }

    pub fn schema_table_up(&mut self, total_rows: usize, _visible_height: usize) -> bool {
        if total_rows == 0 || self.schema_selected_index == 0 {
            return false;
        }
        self.schema_selected_index -= 1;
        if self.schema_selected_index < self.schema_scroll_offset {
            self.schema_scroll_offset = self.schema_selected_index;
        }
        let local = self
            .schema_selected_index
            .saturating_sub(self.schema_scroll_offset);
        self.schema_table_state.select(Some(local));
        true
    }

    /// Sync table state from selected_index/offset (e.g. after tab switch or total_rows change).
    pub fn sync_schema_table_state(&mut self, total_rows: usize, visible_height: usize) {
        if total_rows == 0 {
            self.schema_table_state.select(None);
            return;
        }
        let max_idx = total_rows.saturating_sub(1);
        self.schema_selected_index = self.schema_selected_index.min(max_idx);
        if self.schema_scroll_offset + visible_height <= self.schema_selected_index
            && visible_height > 0
        {
            self.schema_scroll_offset = self.schema_selected_index + 1 - visible_height;
        }
        if self.schema_selected_index < self.schema_scroll_offset {
            self.schema_scroll_offset = self.schema_selected_index;
        }
        let local = self
            .schema_selected_index
            .saturating_sub(self.schema_scroll_offset);
        self.schema_table_state.select(Some(local));
    }
}

/// What the open file says beyond its rows: its size and, where its reader has a facts
/// read (`crate::formats::readers::Reader::facts`), its tab and the footer behind it. Read on a
/// worker once per dataset: a stat or footer read on a dead mount hangs its thread.
#[derive(Debug, Clone)]
pub enum FileFacts {
    /// Asked for; the worker has not answered.
    Reading,
    /// What the worker found.
    Read {
        /// `None` for a directory, whose own size is not the data's.
        size: Option<u64>,
        /// A footer that gives the Schema tab's Compression column; `None` for a
        /// format without one.
        footer: Option<Footer>,
        /// The format's tab, made from what the read found.
        detail: Option<Arc<crate::formats::text_formats::Detail>>,
    },
    /// The read failed, and why. Kept for the dataset rather than asked again: a file
    /// that could not be read a moment ago is not worth a read per frame.
    Failed(String),
}

impl FileFacts {
    /// Stat `path` and, with `facts`, read its format's facts. Blocking: call on a worker.
    /// Failures are short for the panel's line; the full error is logged.
    pub(crate) fn read(
        path: &Path,
        facts: Option<crate::formats::readers::Facts>,
    ) -> std::result::Result<Self, String> {
        let io = |e: std::io::Error| {
            log::warn!(target: "datui", "file size of {}: {e}", path.display());
            match e.kind() {
                std::io::ErrorKind::NotFound => "file not found".to_string(),
                std::io::ErrorKind::PermissionDenied => "permission denied".to_string(),
                // The OS's own words, without the errno the log already has.
                _ => {
                    let said = e.to_string();
                    match said.rsplit_once(" (os error") {
                        Some((words, _)) => words.to_string(),
                        None => said,
                    }
                }
            }
        };
        let meta = std::fs::metadata(path).map_err(io)?;
        if meta.is_dir() {
            return Ok(Self::Read {
                size: None,
                footer: None,
                detail: None,
            });
        }
        let read = match facts {
            Some(facts) => (facts.read)(path).map_err(|e| {
                log::warn!(target: "datui", "footer of {}: {e}", path.display());
                "unreadable footer".to_string()
            })?,
            None => crate::formats::readers::FormatFacts::default(),
        };
        Ok(Self::Read {
            size: Some(meta.len()),
            footer: read.footer,
            detail: read.detail,
        })
    }
}

/// Context for the info panel: the format and what the file says. Open costs belong to
/// the dataset, which the panel already has.
pub struct InfoContext<'a> {
    pub format: Option<ExportFormat>,
    /// The file declares its columns' types, as its format's descriptor says.
    pub declared_types: bool,
    /// `None` when there is no one file on this machine to ask: a remote source, a
    /// glob, or a dataset opened from several paths.
    pub facts: Option<&'a FileFacts>,
    /// The format's tab the facts fill (one local file with a facts read), offered, named
    /// and sized before they land so nothing moves.
    pub facts_tab: Option<&'static str>,
    /// The facts read a footer that gives the Compression column, whose room is kept
    /// while it is read.
    pub footer_expected: bool,
}

impl<'a> InfoContext<'a> {
    /// Where the column types came from, as the Schema rule's chip says it.
    pub fn schema_source(&self) -> &'static str {
        if self.declared_types {
            "types declared"
        } else {
            "types inferred"
        }
    }

    /// The footer the facts read, once it has landed.
    pub fn footer(&self) -> Option<&'a Footer> {
        match self.facts? {
            FileFacts::Read { footer, .. } => footer.as_ref(),
            FileFacts::Reading | FileFacts::Failed(_) => None,
        }
    }

    /// The format's tab the facts made, once it has landed.
    fn facts_detail(&self) -> Option<&'a crate::formats::text_formats::Detail> {
        match self.facts? {
            FileFacts::Read { detail, .. } => detail.as_deref(),
            FileFacts::Reading | FileFacts::Failed(_) => None,
        }
    }

    /// Whether the worker has yet to answer.
    fn reading(&self) -> bool {
        matches!(self.facts, Some(FileFacts::Reading))
    }
}

pub struct DataTableInfo<'a> {
    pub state: &'a DataTableState,
    pub ctx: InfoContext<'a>,
    pub modal: &'a mut InfoModal,
    pub theme: &'a RenderContext,
    /// The dataset is one local file, which `x` shows as hex.
    pub hex: bool,
    /// The dataset is delimited text, whose first row `H` reads the other way.
    pub header_toggle: bool,
    /// What the columns mean, from the catalog that lists the dataset.
    pub codebook: Option<&'a crate::codebook::Codebook>,
    /// The Documentation tab's page, when a catalog lists the dataset.
    pub documentation: Option<&'a mut crate::widgets::documentation::DocState>,
    /// The row count from a sample of the dataset's footers, until it is counted.
    pub estimate: Option<crate::formats::schema_union::RowEstimate>,
}

/// The Resources tab's `Read:` value: how the open reads the data, and that a remote
/// file was downloaded first. `None` for a frame no open found, such as Python's.
fn read_line(state: &DataTableState) -> Option<String> {
    let mode = state.read_mode()?.label();
    Some(if state.fetched() {
        format!("download {} {mode}", crate::glyphs::get().arrow_right)
    } else {
        mode.to_string()
    })
}

/// The Schema tab's first line: the size, or that it is not known yet. `None` rather
/// than an uncounted state's number, which is only how far the buffer reached (`Rows
/// (total): 70` beside a counting spinner).
fn rows_and_columns(rows: Option<usize>, columns: usize) -> String {
    let middot = crate::glyphs::get().middot;
    match rows {
        Some(rows) => format!(
            "Rows (total): {} {middot} Columns: {}",
            format_int(rows),
            columns
        ),
        None => format!("Rows (total): counting... {middot} Columns: {columns}"),
    }
}

/// [`rows_and_columns`] for a count estimated from a sample of footers, with how many
/// were read and the key that counts them all.
fn estimated_rows_and_columns(
    estimate: crate::formats::schema_union::RowEstimate,
    columns: usize,
) -> String {
    let middot = crate::glyphs::get().middot;
    format!(
        "Rows (total): ~{} (est. from {} of {} files; c counts) {middot} Columns: {columns}",
        crate::discover::format_rows(estimate.rows as usize),
        format_int(estimate.sampled),
        format_int(estimate.files),
    )
}

impl<'a> DataTableInfo<'a> {
    pub fn new(
        state: &'a DataTableState,
        ctx: InfoContext<'a>,
        modal: &'a mut InfoModal,
        theme: &'a RenderContext,
    ) -> Self {
        Self {
            state,
            ctx,
            modal,
            theme,
            hex: false,
            header_toggle: false,
            codebook: None,
            documentation: None,
            estimate: None,
        }
    }

    /// The codebook, when it has a note for one of the columns on screen.
    fn codebook_here(&self) -> Option<&'a crate::codebook::Codebook> {
        self.codebook
            .filter(|book| book.covers(self.state.schema().iter_names().map(|n| n.as_str())))
    }

    fn render_schema_tab(&mut self, area: Rect, buf: &mut Buffer) {
        let summary = self.render_schema_summary(area, buf);
        let mut rest = Rect {
            y: area.y + summary,
            height: area.height.saturating_sub(summary),
            ..area
        };
        if rest.height == 0 {
            return;
        }
        // The selected column's note in full below the table: a blank row and three of
        // text, kept whichever column is selected so nothing moves.
        let note_rows = if self.codebook_here().is_some() && rest.height >= 10 {
            4
        } else {
            0
        };
        rest.height -= note_rows;
        let used = self.render_schema_table(rest, buf);
        if note_rows > 0 {
            // Right under the table's last row, where the eye already is.
            self.render_column_note(
                Rect {
                    y: rest.y + used + 1,
                    height: note_rows - 1,
                    ..rest
                },
                buf,
            );
        }
    }

    /// What the codebook says of the selected column: its meaning and unit, then its
    /// codes.
    fn render_column_note(&self, area: Rect, buf: &mut Buffer) {
        let Some(book) = self.codebook_here() else {
            return;
        };
        let Some((name, _)) = self
            .state
            .schema()
            .get_at_index(self.modal.schema_selected_index)
        else {
            return;
        };
        let width = area.width as usize;
        let rows = area.height as usize;
        let mut lines: Vec<String> = Vec::new();
        match book.column(name.as_str()) {
            Some(column) => {
                let about = column.about();
                if !about.is_empty() {
                    lines.extend(wrap_to(&format!("{name}: {about}"), width));
                }
                if !column.values.is_empty() {
                    let sep = format!(" {} ", crate::glyphs::get().middot);
                    let codes: Vec<&str> = column
                        .values
                        .keys()
                        .map(|k| if k.is_empty() { "blank" } else { k.as_str() })
                        .collect();
                    let left = rows.saturating_sub(lines.len()).max(1);
                    let mut wrapped = wrap_to(&format!("Codes: {}", codes.join(&sep)), width);
                    if wrapped.len() > left {
                        // The last line that fits carries the rest, cut with the ellipsis.
                        let rest = wrapped[left - 1..].join(" ");
                        wrapped.truncate(left - 1);
                        wrapped.push(crate::glyphs::fit(&rest, width));
                    }
                    lines.extend(wrapped);
                }
            }
            None => lines.push(format!("{name}: not documented")),
        }
        for (i, line) in lines.iter().take(rows).enumerate() {
            Paragraph::new(crate::glyphs::fit(line, width))
                .style(Style::default().fg(self.theme.text_secondary))
                .render(
                    Rect {
                        y: area.y + i as u16,
                        height: 1,
                        ..area
                    },
                    buf,
                );
        }
    }

    fn render_schema_summary(&self, area: Rect, buf: &mut Buffer) -> u16 {
        let ncols = self.state.schema().len();
        let mut lines = vec![];
        // `num_rows_if_valid`, not `num_rows`: see `rows_and_columns`.
        lines.push(match self.estimate {
            Some(estimate) => estimated_rows_and_columns(estimate, ncols),
            None => rows_and_columns(self.state.num_rows_if_valid(), ncols),
        });
        let by_type = columns_by_type(self.state.schema().as_ref());
        if !by_type.is_empty() {
            lines.push(by_type);
        }
        // A file of several tables (an NMEA log's sentence types) names the others.
        let others = self.state.other_tables();
        if !others.is_empty() {
            let sep = format!(" {} ", crate::glyphs::get().middot);
            lines.push(format!("Other tables (--table): {}", others.join(&sep)));
        }
        if let Some(book) = self.codebook_here()
            && !book.source.is_empty()
        {
            lines.push(crate::glyphs::fit(
                &format!("Documentation: {}", book.source),
                area.width as usize,
            ));
        }
        for (i, s) in lines.iter().enumerate() {
            Paragraph::new(s.as_str()).render(
                Rect {
                    x: area.x,
                    y: area.y + i as u16,
                    width: area.width,
                    height: 1,
                },
                buf,
            );
        }
        lines.len() as u16
    }

    /// Draw the schema table; returns the rows it drew, rule and header included.
    fn render_schema_table(&mut self, area: Rect, buf: &mut Buffer) -> u16 {
        // A dataset of many files says which footers its columns came from; one file
        // says only whether its format declared them.
        let dataset = self.state.dataset_schema();
        let src = match dataset {
            Some(dataset) => dataset.origin.to_string(),
            // A model's, an audio file's or MIDI's columns are datui's own.
            None if self.state.format_detail().is_some_and(|d| d.own_columns) => {
                "types declared".to_string()
            }
            None => self.ctx.schema_source().to_string(),
        };
        // Per column, how many read footers carry it; only multi-file datasets vary (a single
        // file's fact is in the block title).
        let presence = dataset.map(|dataset| {
            let readable = dataset.files.saturating_sub(dataset.unreadable.len());
            let present_by_name: HashMap<&str, usize> = dataset
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.present_in))
                .collect();
            (readable, present_by_name)
        });
        let has_files = presence.is_some();
        let compression = self.ctx.footer().map(|m| {
            crate::formats::parquet_footer::column_compression(
                m.as_ref(),
                self.state.schema().as_ref(),
            )
        });
        // Kept for a file whose footer is still out, so the columns do not re-proportion
        // when it lands.
        let has_comp =
            self.ctx.footer_expected || compression.as_ref().is_some_and(|c| !c.is_empty());
        // A delimited spec's unit row: each column's unit, beside its type.
        let has_units = !self.state.units().is_empty();
        let book = self.codebook_here();
        let mut header_cells = vec!["Column", "Type"];
        if has_units {
            header_cells.push("Unit");
        }
        if book.is_some() {
            header_cells.push("About");
        }
        if has_files {
            header_cells.push("Files");
        }
        if has_comp {
            header_cells.push("Compression");
        }
        let header = Row::new(header_cells).bold();

        let total_rows = self.state.schema().len();
        // The body always has the keys: the tabs switch from anywhere, so the
        // rule is accented and the row carries the rail.
        let body_focused = true;
        // A noun on the rule, and where its types came from in the chip, as every
        // section rule says a fact about its section.
        SectionRule {
            title: "Schema",
            chip: Some(&src),
        }
        .render(Rect { height: 1, ..area }, buf, self.theme);
        let inner = Rect {
            y: area.y + 1,
            height: area.height.saturating_sub(1),
            ..area
        };
        let visible_height = inner.height as usize;

        // One header row, plus one for the out-of-view count when columns overflow, so hidden
        // columns are stated before scrolling.
        let fits = total_rows <= visible_height.saturating_sub(1);
        let data_height = visible_height.saturating_sub(1 + usize::from(!fits));
        self.modal.schema_visible_height = data_height;
        self.modal.sync_schema_table_state(total_rows, data_height);

        let offset = self.modal.schema_scroll_offset;
        let take = data_height.min(total_rows.saturating_sub(offset));
        let mut rows: Vec<Vec<String>> = vec![];
        for (idx, (name, dtype)) in self.state.schema().iter().enumerate() {
            if idx < offset {
                continue;
            }
            if idx >= offset + take {
                break;
            }
            let name_str: &str = name.as_ref();
            let mut cells = vec![name.to_string(), dtype.to_string()];
            if has_units {
                cells.push(self.state.unit_of(name_str).unwrap_or_default().to_string());
            }
            if let Some(book) = book {
                cells.push(book.column(name_str).map(|c| c.about()).unwrap_or_default());
            }
            if let Some((readable, present_by_name)) = &presence {
                // A column the footers never named — one built by a query or added
                // from the file names — has no per-file fact to state.
                let files_str = match present_by_name.get(name_str) {
                    Some(present) if present >= readable => {
                        format!("all {}", format_int(*readable))
                    }
                    Some(present) => {
                        format!("{} of {}", format_int(*present), format_int(*readable))
                    }
                    None => crate::glyphs::get().dash.to_string(),
                };
                cells.push(files_str);
            }
            if has_comp {
                let comp_str = match compression.as_ref().map(|c| c.get(name_str)) {
                    Some(Some((codec, ratio))) => {
                        format!("{} {:.1}{}", codec, ratio, crate::glyphs::get().times)
                    }
                    // Blank until the footer lands, rather than a dash that says it did.
                    None if self.ctx.reading() => String::new(),
                    _ => crate::glyphs::get().dash.to_string(),
                };
                cells.push(comp_str);
            }
            rows.push(cells);
        }

        let widths: Vec<Constraint> = if book.is_some() {
            // The note takes what the name and type leave.
            let mut weights = vec![3, 2];
            weights.extend(has_units.then_some(2));
            weights.push(7);
            weights.extend(has_files.then_some(2));
            weights.extend(has_comp.then_some(3));
            weights.into_iter().map(Constraint::Fill).collect()
        } else if has_units {
            // Name and type as wide as each other, the rest narrower.
            let mut weights = vec![3, 3, 2];
            weights.extend(has_files.then_some(2));
            weights.extend(has_comp.then_some(3));
            weights.into_iter().map(Constraint::Fill).collect()
        } else {
            match (has_files, has_comp) {
                (true, true) => vec![
                    Constraint::Percentage(25),
                    Constraint::Percentage(30),
                    Constraint::Percentage(20),
                    Constraint::Percentage(25),
                ],
                (true, false) => vec![
                    Constraint::Percentage(35),
                    Constraint::Percentage(40),
                    Constraint::Percentage(25),
                ],
                (false, true) => vec![
                    Constraint::Percentage(30),
                    Constraint::Percentage(40),
                    Constraint::Percentage(30),
                ],
                (false, false) => vec![Constraint::Percentage(50), Constraint::Percentage(50)],
            }
        };
        // Rail and tint while focused; accent alone otherwise, the rail's column kept so
        // focus moves nothing.
        let g = crate::glyphs::get();
        let (highlight, symbol) = if body_focused {
            (self.theme.highlight_style(), g.selector)
        } else {
            (Style::default().fg(self.theme.accent), g.selector_blank)
        };
        let symbol = Span::styled(symbol, Style::default().fg(self.theme.accent));
        // A note longer than its column ends in the ellipsis rather than mid-word: the
        // whole of it is under the table.
        if book.is_some() {
            let about = 2 + usize::from(has_units);
            let room = Rect {
                width: inner
                    .width
                    .saturating_sub(crate::glyphs::cell_width(g.selector) as u16),
                ..inner
            };
            let cols = Layout::horizontal(widths.clone()).spacing(1).split(room);
            if let Some(col) = cols.get(about) {
                for cells in &mut rows {
                    if let Some(cell) = cells.get_mut(about) {
                        *cell = crate::glyphs::fit(cell, col.width as usize);
                    }
                }
            }
        }
        let rows: Vec<Row> = rows.into_iter().map(Row::new).collect();
        let table = Table::new(rows, widths)
            .header(header)
            .column_spacing(1)
            .row_highlight_style(highlight)
            .highlight_symbol(symbol)
            .highlight_spacing(HighlightSpacing::Always);
        let table_area = Rect {
            height: inner.height.saturating_sub(u16::from(!fits)),
            ..inner
        };
        StatefulWidget::render(table, table_area, buf, &mut self.modal.schema_table_state);

        if !fits && inner.height > 0 {
            let above = offset;
            let below = total_rows.saturating_sub(offset + take);
            let counted = match (above, below) {
                (0, 0) => None,
                (0, n) => Some(format!("{} below", format_int(n))),
                (n, 0) => Some(format!("{} above", format_int(n))),
                (a, b) => Some(format!("{} above, {} below", format_int(a), format_int(b))),
            };
            if let Some(text) = counted {
                Paragraph::new(text)
                    .style(Style::default().fg(self.theme.dimmed))
                    .alignment(ratatui::layout::Alignment::Right)
                    .render(
                        Rect {
                            y: inner.y + inner.height - 1,
                            height: 1,
                            ..inner
                        },
                        buf,
                    );
            }
        }
        // The rule, the header, the rows and the count under them.
        (2 + take + usize::from(!fits)).min(area.height as usize) as u16
    }

    fn render_resources_tab(&self, area: Rect, buf: &mut Buffer) {
        // One past the longest label, "Buffer (Rows):", with room to spare.
        const LABEL_WIDTH: u16 = 17;
        let label_constraint = Constraint::Length(LABEL_WIDTH);
        let value_constraint = Constraint::Min(1);
        let mut y = area.y;
        let h = area.height;
        let w = area.width;

        fn label_value_row(label: &str, value: &str, area: Rect, buf: &mut Buffer, label_w: u16) {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Length(label_w), Constraint::Min(1)])
                .split(area);
            Paragraph::new(label).render(chunks[0], buf);
            Paragraph::new(value).render(chunks[1], buf);
        }

        if y >= area.y + h {
            return;
        }
        let size_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([label_constraint, value_constraint])
            .split(Rect {
                y,
                width: w,
                height: 1,
                ..area
            });
        // Drawn from what the worker left; reading the file here would hang the frame
        // on a mount that has stopped answering.
        let file_size = match self.ctx.facts {
            None | Some(FileFacts::Read { size: None, .. }) => Span::raw(crate::glyphs::get().dash),
            Some(FileFacts::Read {
                size: Some(size), ..
            }) => Span::raw(crate::numfmt::bytes(*size)),
            Some(FileFacts::Reading) => {
                Span::styled("reading...", Style::default().fg(self.theme.dimmed))
            }
            Some(FileFacts::Failed(why)) => Span::styled(
                crate::glyphs::fit(why, size_chunks[1].width as usize),
                Style::default().fg(self.theme.error),
            ),
        };
        Paragraph::new("File size:").render(size_chunks[0], buf);
        Paragraph::new(Line::from(file_size)).render(size_chunks[1], buf);
        y += 1;

        if y >= area.y + h {
            return;
        }
        let fmt = self
            .ctx
            .format
            .map(|f| f.as_str())
            .unwrap_or(crate::glyphs::get().dash);
        label_value_row(
            "Format:",
            fmt,
            Rect {
                y,
                width: w,
                height: 1,
                ..area
            },
            buf,
            LABEL_WIDTH,
        );
        y += 1;

        // How the open reads it: whether scrolling reads the file or memory.
        if let Some(read) = read_line(self.state) {
            if y >= area.y + h {
                return;
            }
            label_value_row(
                "Read:",
                &read,
                Rect {
                    y,
                    width: w,
                    height: 1,
                    ..area
                },
                buf,
                LABEL_WIDTH,
            );
            y += 1;
        }

        if y >= area.y + h {
            return;
        }
        let buf_rows = self.state.buffered_rows();
        let max_rows = self.state.max_buffered_rows();
        let row_area = Rect {
            y,
            width: w,
            height: 1,
            ..area
        };
        let row_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([label_constraint, value_constraint])
            .split(row_area);
        Paragraph::new("Buffer (Rows):").render(row_chunks[0], buf);
        // Values start in the value column, as every other row's do.
        if max_rows > 0 {
            let label = format!("{} / {}", format_int(buf_rows), format_int(max_rows));
            Paragraph::new(label).render(row_chunks[1], buf);
        } else {
            Paragraph::new(format_int(buf_rows)).render(row_chunks[1], buf);
        }
        y += 1;

        if y >= area.y + h {
            return;
        }
        let buf_mb = self
            .state
            .buffered_memory_bytes()
            .map(|b| b / (1024 * 1024));
        let max_mb = self.state.max_buffered_mb();
        let mb_area = Rect {
            y,
            width: w,
            height: 1,
            ..area
        };
        let mb_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([label_constraint, value_constraint])
            .split(mb_area);
        Paragraph::new("Buffer (MB):").render(mb_chunks[0], buf);
        if max_mb > 0 {
            let label = match buf_mb {
                Some(m) => format!("{:.1} / {} MiB", m as f64, max_mb),
                None => crate::glyphs::get().dash.to_string(),
            };
            Paragraph::new(label).render(mb_chunks[1], buf);
        } else {
            let value = buf_mb
                .map(|m| format!("{:.1} MiB", m as f64))
                .unwrap_or_else(|| {
                    self.state
                        .buffered_memory_bytes()
                        .map(|b| crate::numfmt::bytes(b as u64))
                        .unwrap_or_else(|| crate::glyphs::get().dash.to_string())
                });
            Paragraph::new(value).render(mb_chunks[1], buf);
        }
        y += 1;

        self.render_measurements(area, buf, &mut y, LABEL_WIDTH);
    }

    /// What the open cost, under its heading at the tab's foot. Only what was measured: no
    /// zeros for figures datui cannot stand behind (see [`crate::measurements`] and
    /// `docs/user-guide/dataset-info.md`).
    fn render_measurements(&self, area: Rect, buf: &mut Buffer, y: &mut u16, label_w: u16) {
        let meter = self.state.measurements();
        let mut rows: Vec<(&str, String)> = [
            ("Listing:", "files", "file", meter.listing()),
            ("Footers:", "footers read", "footer read", meter.footers()),
            ("Last page:", "files read", "file read", meter.last_page()),
        ]
        .into_iter()
        .filter_map(|(label, unit, singular, cost)| {
            Some((label, measurement_line(&cost?, unit, singular)))
        })
        .collect();
        if let Some(total) = meter.total() {
            rows.push(("Total:", total_line(&total)));
        }
        if rows.is_empty() {
            return;
        }
        let bottom = area.y + area.height;
        // The heading with at least one row, or neither: blank at `y`, heading at `y + 1`,
        // first row at `y + 2` must all fit.
        if *y + 2 >= bottom {
            return;
        }
        *y += 1;
        SectionRule {
            title: "Measurements",
            chip: None,
        }
        .render(
            Rect {
                y: *y,
                width: area.width,
                height: 1,
                ..area
            },
            buf,
            self.theme,
        );
        *y += 1;
        for (label, line) in rows {
            if *y >= bottom {
                return;
            }
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Length(label_w), Constraint::Min(1)])
                .split(Rect {
                    y: *y,
                    width: area.width,
                    height: 1,
                    ..area
                });
            Paragraph::new(label).render(chunks[0], buf);
            Paragraph::new(line).render(chunks[1], buf);
            *y += 1;
        }
    }

    /// A delimited spec's metadata line: its title, then each key and value. A line
    /// that is not key=value pairs is shown as it is.
    fn render_metadata_tab(&mut self, area: Rect, buf: &mut Buffer) {
        let Some(read) = self.state.delimited_read().cloned() else {
            return;
        };
        let Some(metadata) = &read.metadata else {
            return;
        };
        if area.height == 0 || area.width < 8 {
            return;
        }
        let mut lines = Vec::new();
        if let Some(title) = &metadata.title {
            lines.push((title.clone(), Style::default()));
        }
        if let Some(file) = &read.facts_from {
            lines.push((format!("From {file}"), Style::default()));
        }
        let shown: Vec<(String, crate::formats::model_files::MetaValue)> =
            if metadata.pairs.is_empty() {
                let line = read.delimited().metadata_line.unwrap_or(1);
                vec![(
                    format!("line {line}"),
                    crate::formats::model_files::MetaValue::Text(metadata.raw.clone()),
                )]
            } else {
                metadata
                    .pairs
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            crate::formats::model_files::MetaValue::Text(v.clone()),
                        )
                    })
                    .collect()
            };
        self.render_detail(area, buf, &lines, "Metadata", &shown);
    }

    /// A file's lines, its warnings, then its list: signals, tags, metadata, tracks.
    fn render_format_tab(&mut self, area: Rect, buf: &mut Buffer) {
        let state = self.state;
        let Some(detail) = state.format_detail().or_else(|| self.ctx.facts_detail()) else {
            // The facts that fill it are still out, or could not be read.
            let (said, style) = match self.ctx.facts {
                Some(FileFacts::Failed(why)) => {
                    (why.clone(), Style::default().fg(self.theme.error))
                }
                _ => (
                    "reading...".to_string(),
                    Style::default().fg(self.theme.dimmed),
                ),
            };
            Paragraph::new(crate::glyphs::fit(&said, area.width as usize))
                .style(style)
                .render(
                    Rect {
                        height: area.height.min(1),
                        ..area
                    },
                    buf,
                );
            return;
        };
        let warn = Style::default().fg(self.theme.warning);
        let lines: Vec<(String, Style)> = detail
            .lines
            .iter()
            .map(|line| (line.clone(), Style::default()))
            .chain(detail.warnings.iter().map(|line| (line.clone(), warn)))
            .collect();
        let pick = !detail.tables.is_empty();
        self.render_detail_list(area, buf, &lines, detail.list_title, &detail.list, pick);
    }

    /// A detail tab: head lines, a blank, a rule titled `title`, then the key/value list
    /// scrolled by `detail_scroll`, each value wrapped whole.
    fn render_detail(
        &mut self,
        area: Rect,
        buf: &mut Buffer,
        lines: &[(String, Style)],
        title: &str,
        list: &[(String, crate::formats::model_files::MetaValue)],
    ) {
        self.render_detail_list(area, buf, lines, title, list, false);
    }

    /// [`Self::render_detail`]; with `pick`, the list has a cursor on one entry
    /// (`detail_selected`), drawn with the rail, which the scroll follows.
    fn render_detail_list(
        &mut self,
        area: Rect,
        buf: &mut Buffer,
        lines: &[(String, Style)],
        title: &str,
        list: &[(String, crate::formats::model_files::MetaValue)],
        pick: bool,
    ) {
        if area.height == 0 || area.width < 8 {
            return;
        }
        let width = area.width as usize;
        let mut y = area.y;
        let bottom = area.y + area.height;
        // Wrapped rather than cut: a warning cut off mid-sentence says less than nothing.
        for (line, style) in lines {
            for part in wrap_to(line, width) {
                if y >= bottom {
                    return;
                }
                Paragraph::new(crate::glyphs::fit(&part, width))
                    .style(*style)
                    .render(
                        Rect {
                            y,
                            height: 1,
                            ..area
                        },
                        buf,
                    );
                y += 1;
            }
        }
        // A blank line, then the rule, then at least one line of the list, or none: an
        // empty list has no rule to count it.
        if list.is_empty() || y + 2 >= bottom {
            return;
        }
        y += 1;
        let count = group_u64(list.len() as u64);
        SectionRule {
            title,
            chip: Some(&count),
        }
        .render(
            Rect {
                y,
                height: 1,
                ..area
            },
            buf,
            self.theme,
        );
        y += 1;

        // A list with a cursor keeps a column for its rail, so the cursor arriving
        // moves nothing.
        let gutter = u16::from(pick);
        let rows = metadata_lines(list, width.saturating_sub(gutter as usize));
        let room = (bottom - y) as usize;
        let fits = rows.len() <= room;
        // The last row says what is out of view when not everything fits.
        let shown = if fits { room } else { room.saturating_sub(1) };
        self.modal.detail_visible = shown;
        let max_scroll = rows.len().saturating_sub(shown);
        // The lines of the entry under the cursor: an entry's first line names it,
        // the lines a long value wraps onto leave its key blank.
        let picked = pick.then(|| {
            let starts: Vec<usize> = (0..rows.len())
                .filter(|&i| !rows[i].0.trim().is_empty())
                .collect();
            let at = self
                .modal
                .detail_selected
                .min(starts.len().saturating_sub(1));
            self.modal.detail_selected = at;
            let start = starts.get(at).copied().unwrap_or(0);
            let end = starts.get(at + 1).copied().unwrap_or(rows.len());
            if start < self.modal.detail_scroll {
                self.modal.detail_scroll = start;
            } else if end > self.modal.detail_scroll + shown {
                self.modal.detail_scroll = end.saturating_sub(shown).min(start);
            }
            start..end
        });
        self.modal.detail_scroll = self.modal.detail_scroll.min(max_scroll);
        let first = self.modal.detail_scroll;
        let key_style = Style::default().fg(self.theme.text_secondary);
        let rail = crate::glyphs::get().rail;
        for (i, (key, value)) in rows.iter().enumerate().skip(first).take(shown) {
            let row = Rect {
                y,
                height: 1,
                ..area
            };
            let on = picked.as_ref().is_some_and(|p| p.contains(&i));
            let (key_style, value_style) = if on {
                let hl = self.theme.highlight_style();
                (key_style.patch(hl), Style::default().patch(hl))
            } else {
                (key_style, Style::default())
            };
            if on && picked.as_ref().is_some_and(|p| p.start == i) {
                buf.set_string(row.x, row.y, rail, Style::default().fg(self.theme.accent));
            }
            Paragraph::new(Line::from(vec![
                Span::styled(key.clone(), key_style),
                Span::styled(value.clone(), value_style),
            ]))
            .render(
                Rect {
                    x: row.x + gutter,
                    width: row.width.saturating_sub(gutter),
                    ..row
                },
                buf,
            );
            y += 1;
        }
        if !fits && shown > 0 {
            let above = first;
            let below = rows.len().saturating_sub(first + shown);
            let text = match (above, below) {
                (0, n) => format!("{} below", group_chrome(n)),
                (n, 0) => format!("{} above", group_chrome(n)),
                (a, b) => format!("{} above, {} below", group_chrome(a), group_chrome(b)),
            };
            Paragraph::new(text)
                .style(Style::default().fg(self.theme.dimmed))
                .alignment(ratatui::layout::Alignment::Right)
                .render(
                    Rect {
                        y: bottom - 1,
                        height: 1,
                        ..area
                    },
                    buf,
                );
        }
    }

    /// What datui noticed: each note's summary and its basis line. Whole notes only (a
    /// claim without its basis, or the reverse, misleads), as [`notes_window`] ensures.
    /// Plain styling: observations, not alarms.
    fn render_notes_tab(&mut self, area: Rect, buf: &mut Buffer) {
        let notes = self.state.notes();
        if area.height == 0 || area.width <= 4 || notes.is_empty() {
            return;
        }
        let selected = self.modal.notes_selected_index.min(notes.len() - 1);
        let width = area.width as usize;
        let blocks: Vec<Vec<NoteRow>> = notes
            .iter()
            .enumerate()
            .map(|(index, note)| note_rows(note, index == selected, width))
            .collect();
        let heights: Vec<usize> = blocks.iter().map(Vec::len).collect();
        let dim = Style::default().fg(self.theme.dimmed);

        // Try the whole panel first; reserve a count row only when notes are left out.
        let full = area.height as usize;
        let (first, last) = notes_window(&heights, selected, self.modal.notes_scroll_offset, full);
        let all_shown = first == 0 && last == heights.len();
        // A row for the hidden count only when something is hidden and the note can spare it,
        // and for the read-as-text offer, without which the note is half a note.
        let offer = notes[selected]
            .read_as_text
            .as_ref()
            .map(|column| format!("Enter  read {column} as text"));
        let reserve = (!all_shown || offer.is_some()) && heights[selected] < full;
        let show = if reserve { full - 1 } else { full };
        if heights[selected] > show {
            // The selected note cannot show summary and basis: say so ("this one": another note
            // may fit).
            let count = notes.len();
            Paragraph::new(Line::from(Span::styled(
                format!(
                    "{} {} {} selected: no room",
                    group_chrome(count),
                    if count == 1 { "note" } else { "notes" },
                    crate::glyphs::get().middot
                ),
                dim,
            )))
            .render(Rect { height: 1, ..area }, buf);
            return;
        }
        let (first, last) = if reserve {
            notes_window(&heights, selected, self.modal.notes_scroll_offset, show)
        } else {
            (first, last)
        };
        self.modal.notes_scroll_offset = first;

        let mut y = area.y;
        let bottom = area.y + show as u16;
        for (offset, block) in blocks[first..last].iter().enumerate() {
            if offset > 0 && y < bottom {
                y += 1;
            }
            for row in block {
                if y >= bottom {
                    break;
                }
                let at = Rect {
                    y,
                    height: 1,
                    ..area
                };
                if row.dim {
                    Paragraph::new(Line::from(Span::styled(row.text.clone(), dim))).render(at, buf);
                } else {
                    Paragraph::new(row.text.as_str()).render(at, buf);
                }
                y += 1;
            }
        }

        // The offer and the hidden count share the last row: the short count first, the offer
        // gets the rest, so neither overwrites the other.
        let (above, below) = (first, notes.len() - last);
        let hidden = match (reserve, above, below) {
            (false, _, _) | (_, 0, 0) => None,
            (_, 0, n) => Some(format!("{} below", group_chrome(n))),
            (_, n, 0) => Some(format!("{} above", group_chrome(n))),
            (_, a, b) => Some(format!(
                "{} above, {} below",
                group_chrome(a),
                group_chrome(b)
            )),
        };
        if !reserve || (hidden.is_none() && offer.is_none()) {
            return;
        }
        let last_row = Rect {
            y: area.y + area.height - 1,
            height: 1,
            ..area
        };
        // A space between them, so they never read as one phrase when both are there.
        use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};
        let taken = hidden
            .as_ref()
            .map(|text| (text.width() as u16).saturating_add(1))
            .unwrap_or(0);
        if let Some(offer) = offer.as_ref() {
            let room = last_row.width.saturating_sub(taken) as usize;
            // Cut with a mark, never silently: a truncated offer can read as a different one.
            let offer = if offer.width() > room {
                // Cut by width rather than by word: the spacing after the key name is
                // part of how the line reads, and wrapping would close it up.
                let mark = crate::glyphs::get().ellipsis;
                let mut kept = String::new();
                for ch in offer.chars() {
                    if kept.width() + ch.width().unwrap_or(0) + mark.width() > room {
                        break;
                    }
                    kept.push(ch);
                }
                Some(format!("{kept}{mark}"))
            } else {
                Some(offer.clone())
            };
            // Below about a word there is no offer left to make, only the mark.
            if let Some(offer) = offer.filter(|_| room >= 8) {
                Paragraph::new(Line::from(Span::styled(offer, dim))).render(
                    Rect {
                        width: room as u16,
                        ..last_row
                    },
                    buf,
                );
            }
        }
        if let Some(hidden) = hidden {
            Paragraph::new(Line::from(Span::styled(hidden, dim)))
                .right_aligned()
                .render(last_row, buf);
        }
    }

    fn render_partitioned_data_tab(&self, area: Rect, buf: &mut Buffer) {
        let y = area.y;
        let w = area.width;

        let Some(partition_columns) = self.state.partition_columns() else {
            Paragraph::new("Partition columns: unknown").render(
                Rect {
                    y,
                    width: w,
                    height: 1,
                    ..area
                },
                buf,
            );
            return;
        };

        if partition_columns.is_empty() {
            Paragraph::new("Partition columns: none").render(
                Rect {
                    y,
                    width: w,
                    height: 1,
                    ..area
                },
                buf,
            );
            return;
        }

        let line = format!("Partition columns: {}", partition_columns.join(", "));
        Paragraph::new(line).render(
            Rect {
                y,
                width: w,
                height: 1,
                ..area
            },
            buf,
        );
    }
}

/// How long a stretch took without rounding it away: milliseconds to two places under a
/// second (seconds would print `0.00s` for most), floor `0.00 ms` under five
/// microseconds.
fn format_took(took: std::time::Duration) -> String {
    let ms = took.as_secs_f64() * 1000.0;
    // Round to the printed places before choosing the unit, so 999.997 ms prints `1.00s`,
    // not `1000.00 ms`.
    if (ms * 100.0).round() < 100_000.0 {
        format!("{ms:.2} ms")
    } else {
        format!("{:.2}s", took.as_secs_f64())
    }
}

/// What datui asked for over a network, where it did the asking.
fn wire_line(wire: crate::measurements::OverTheWire) -> String {
    let mut line = format!(
        ", {} request{}",
        format_int(wire.requests),
        if wire.requests == 1 { "" } else { "s" }
    );
    // Bytes only where datui counted them, so an unweighed stretch never prints `0 B`.
    if let Some(bytes) = wire.bytes {
        line.push_str(&format!(", {}", crate::numfmt::bytes(bytes)));
    }
    line
}

/// One measurement as a line: time, count of what it counted, and requests and bytes
/// where datui made them. `unit` varies: footer passes count footers, which may
/// exceed the dataset's files.
fn measurement_line(cost: &crate::measurements::Cost, unit: &str, singular: &str) -> String {
    let mut line = format_took(cost.took);
    // Without a count, just the time: no number that is not the dataset's size.
    if let Some(files) = cost.files {
        let unit = if files == 1 { singular } else { unit };
        line.push_str(&format!(", {} {unit}", format_int(files)));
    }
    if let Some(wire) = cost.over_the_wire {
        line.push_str(&wire_line(wire));
    }
    line
}

/// The total as a line: time and requests, no file count (see
/// [`crate::measurements::Meter::total`]).
fn total_line(total: &crate::measurements::Total) -> String {
    let mut line = format_took(total.took);
    if let Some(wire) = total.over_the_wire {
        line.push_str(&wire_line(wire));
    }
    line
}

/// Comma-group a count for the info panel. Thin alias over the shared chrome
/// formatter, kept so call sites and tests read the same as before.
fn format_int(n: usize) -> String {
    crate::numfmt::group_chrome(n)
}

fn columns_by_type(schema: &Schema) -> String {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for (_, dtype) in schema.iter() {
        let k = dtype.to_string();
        *counts.entry(k).or_default() += 1;
    }
    let mut pairs: Vec<_> = counts.into_iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
        .into_iter()
        .map(|(k, v)| format!("{}: {}", k, v))
        .collect::<Vec<_>>()
        .join(&format!(" {} ", crate::glyphs::get().middot))
}

impl<'a> Widget for &mut DataTableInfo<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let ctx = self.theme;
        let offered = TabsOffered {
            documentation: self.documentation.is_some(),
            ..TabsOffered::of(self.state, self.ctx.facts_tab)
        };
        let tab = self.modal.active_tab;

        // The panel's own keys, said where they work and only while they work:
        // nothing here may live only in `?`.
        let g = crate::glyphs::get();
        let scrolls = match tab {
            InfoTab::Schema => true,
            InfoTab::Notes => offered.notes,
            InfoTab::Metadata => offered.metadata,
            InfoTab::Format => offered.format,
            InfoTab::Documentation => offered.documentation,
            _ => false,
        };
        // A tab whose list is the file's tables has a cursor, and Enter opens one.
        let tables = tab == InfoTab::Format
            && offered.format
            && self
                .state
                .format_detail()
                .is_some_and(|d| !d.tables.is_empty());
        let mut footer = HintBar::from_ctx(ctx)
            .screen(datui_cli::keys::Context::Info)
            .key("← / →")
            .weight(3);
        if tables {
            footer = footer.key("Enter").weight(2).key("↑ / ↓").weight(2);
        } else if scrolls {
            footer = footer.key_as("↑ / ↓", "Scroll").weight(2);
        }
        if tab == InfoTab::Documentation && offered.documentation {
            footer = footer.key_as("Enter", "Values").weight(1);
            if self
                .documentation
                .as_deref()
                .is_some_and(|d| d.offers_open())
            {
                footer = footer.key("o").weight(1);
            }
            footer = footer.key("y").weight(1);
        }
        if tab == InfoTab::Schema && self.header_toggle {
            footer = footer.key("H").weight(-1);
        }
        if self.hex {
            footer = footer.key("x").weight(0);
        }
        let footer = footer.key("Esc").weight(4);
        // A frame of three rows has one inside it: the body's, so a panel too
        // short for a note still says so rather than showing only keys.
        let surface = Surface::new("Info");
        let surface = if area.height > 3 {
            surface.footer(&footer)
        } else {
            surface
        };
        let content = surface.render(area, buf, ctx);
        if content.height == 0 || content.width < 4 {
            return;
        }
        // Short of height, the blank row under the tabs goes first, then the tab
        // line: the body is what the panel is for.
        let tab_rows = u16::from(content.height >= 4);
        let gap = u16::from(content.height >= 6);

        // Tab line: the active tab is accented; the bar never takes focus (tabs switch from
        // anywhere), so no rail, slot kept.
        let tabs = InfoTab::visible(offered);
        let active = tabs[tab.index(offered)];
        let current = tab.index(offered);
        let mut spans = Vec::new();
        let mut clicks = Vec::new();
        for (i, t) in tabs.iter().enumerate() {
            let is_active = *t == active;
            if i > 0 {
                spans.push(Span::styled(
                    format!(" {}", g.rule),
                    Style::default().fg(ctx.dimmed),
                ));
            }
            spans.push(Span::raw(" "));
            let style = if is_active {
                Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(ctx.text_secondary)
            };
            // The Format tab is named by the file's detail (VCD, Model, Audio), or by
            // the format's descriptor while the facts that fill it are read.
            let title = match (t, self.state.format_detail()) {
                (InfoTab::Format, Some(detail)) => detail.tab,
                (InfoTab::Format, None) => self.ctx.facts_tab.unwrap_or(t.title()),
                _ => t.title(),
            };
            // A click steps the tabs there, as ← / → do from anywhere here.
            clicks.push((
                spans.len(),
                crate::pointer::Hit::Option {
                    field: None,
                    index: i,
                    current,
                },
            ));
            spans.push(Span::styled(title, style));
        }
        let tab_area = Rect {
            height: tab_rows,
            ..content
        };
        let line = Line::from(spans);
        if tab_rows > 0 {
            crate::pointer::record_spans(tab_area, &line, clicks);
        }
        Paragraph::new(line).render(tab_area, buf);

        // A blank row under the tabs rather than a rule: the tab line is state,
        // not a section.
        let body = Rect {
            y: content.y + tab_rows + gap,
            height: content.height - tab_rows - gap,
            ..content
        };
        match tab {
            InfoTab::Schema => self.render_schema_tab(body, buf),
            InfoTab::Resources => self.render_resources_tab(body, buf),
            InfoTab::Metadata if offered.metadata => self.render_metadata_tab(body, buf),
            InfoTab::Format if offered.format => self.render_format_tab(body, buf),
            InfoTab::Partitions if offered.partitions => {
                self.render_partitioned_data_tab(body, buf)
            }
            InfoTab::Notes if offered.notes => self.render_notes_tab(body, buf),
            InfoTab::Documentation if offered.documentation => {
                if let Some(page) = self.documentation.as_deref_mut() {
                    crate::widgets::documentation::render_page(page, body, buf, ctx);
                }
            }
            InfoTab::Metadata
            | InfoTab::Format
            | InfoTab::Partitions
            | InfoTab::Notes
            | InfoTab::Documentation => self.render_schema_tab(body, buf),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The optional tabs: Partitions and Notes as asked, no others.
    fn offer(partitions: bool, notes: bool) -> TabsOffered {
        TabsOffered {
            metadata: false,
            format: false,
            partitions,
            notes,
            documentation: false,
        }
    }

    /// What a file says about itself: a size and, for a format with a facts read, its
    /// footer and tab; a directory has no size of its own to give; a file that is gone,
    /// or whose footer is not one, is a reason rather than a blank.
    #[test]
    fn file_facts_read_what_each_source_has() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("rows.csv");
        std::fs::write(&csv, "a\n1\n").unwrap();
        let parquet = dir.path().join("rows.parquet");
        let mut df = df!("a" => &[1i64, 2, 3]).unwrap();
        ParquetWriter::new(std::fs::File::create(&parquet).unwrap())
            .finish(&mut df)
            .unwrap();
        let parquet_len = std::fs::metadata(&parquet).unwrap().len();
        let facts = crate::formats::readers::of(crate::FileFormat::Parquet).facts;

        assert!(matches!(
            FileFacts::read(&csv, None),
            Ok(FileFacts::Read {
                size: Some(4),
                footer: None,
                detail: None,
            })
        ));
        match FileFacts::read(&parquet, facts) {
            Ok(FileFacts::Read {
                size: Some(size),
                footer: Some(footer),
                detail: Some(detail),
            }) => {
                assert_eq!(size, parquet_len);
                assert_eq!(footer.num_rows, 3);
                assert_eq!(detail.tab, "Parquet");
            }
            other => panic!("a Parquet file's size, footer and tab: {other:?}"),
        }
        assert!(matches!(
            FileFacts::read(dir.path(), facts),
            Ok(FileFacts::Read {
                size: None,
                footer: None,
                detail: None,
            })
        ));
        // Reasons short enough for the panel's one line.
        assert_eq!(
            FileFacts::read(&dir.path().join("gone.parquet"), facts).unwrap_err(),
            "file not found"
        );
        assert_eq!(
            FileFacts::read(&csv, facts).expect_err("a CSV has no footer"),
            "unreadable footer"
        );
    }

    /// A dataset that has not been counted says so rather than showing how far it got.
    ///
    /// Through a rendered panel, not the helper: the helper cannot tell whether its
    /// caller passed `num_rows_if_valid()` or the raw field, and the raw field is what
    /// the bug was.
    #[test]
    fn the_schema_tab_does_not_call_a_partial_the_total() {
        use crate::table::DataTableState;
        use polars::prelude::*;

        let rows = || df!("id" => (0..70i64).collect::<Vec<_>>()).unwrap().lazy();
        let mut lf = rows();
        let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
        let mut state = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap();
        // As a staged open leaves it: a provisional from however far the buffer reached,
        // with no count taken.
        state.set_provisional_rows(70);

        let theme = RenderContext::for_test();
        let painted = |state: &DataTableState| {
            let area = Rect::new(0, 0, 60, 12);
            let mut buf = Buffer::empty(area);
            let mut modal = InfoModal::default();
            let panel = DataTableInfo::new(
                state,
                InfoContext {
                    format: None,
                    facts: None,
                    facts_tab: None,
                    footer_expected: false,
                    declared_types: false,
                },
                &mut modal,
                &theme,
            );
            panel.render_schema_summary(area, &mut buf);
            crate::tests::buffer_text(&buf)
        };

        let uncounted = painted(&state);
        assert!(
            uncounted.contains("counting..."),
            "a count not taken is not a total: {uncounted}"
        );
        assert!(
            !uncounted.contains("70"),
            "and the buffer's height is not shown in its place: {uncounted}"
        );

        assert!(state.count_landed(state.len_generation(), 70, None));
        let counted = painted(&state);
        assert!(
            counted.contains("Rows (total): 70"),
            "and once it has been counted, that is what it says: {counted}"
        );
    }

    /// A schema taller than the panel says how many columns are out of view
    /// before any scrolling, the selection carries the shared rail, and the
    /// panel names its keys in a footer.
    #[test]
    fn a_tall_schema_counts_its_hidden_columns() {
        use crate::table::DataTableState;
        use polars::prelude::*;

        let wide = || {
            let base = df!("col_0" => &[1i64]).unwrap().lazy();
            let extra: Vec<Expr> = (1..24)
                .map(|i| lit(1i64).alias(format!("col_{i}")))
                .collect();
            base.with_columns(extra)
        };
        let mut lf = wide();
        let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
        let state = DataTableState::from_schema_and_lazyframe(
            schema,
            wide(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap();

        let theme = RenderContext::for_test();
        let area = Rect::new(0, 0, 60, 16);
        let mut buf = Buffer::empty(area);
        let mut modal = InfoModal::default();
        let mut panel = DataTableInfo::new(
            &state,
            InfoContext {
                format: None,
                facts: None,
                facts_tab: None,
                footer_expected: false,
                declared_types: false,
            },
            &mut modal,
            &theme,
        );
        (&mut panel).render(area, &mut buf);
        let text = crate::tests::buffer_text(&buf);
        assert!(
            text.contains("below"),
            "the hidden columns are counted: {text}"
        );
        assert!(text.contains("Esc"), "the footer names the way out: {text}");
        assert!(text.contains("Tabs"), "and the tab keys: {text}");
        assert!(!text.contains(">>"), "the bespoke marker is gone: {text}");
    }

    /// The Schema tab's footer offers `H` for delimited text alone, and no other
    /// tab does.
    #[test]
    fn the_schema_footer_offers_h_only_where_it_works() {
        use crate::table::DataTableState;
        use polars::prelude::*;

        let lf = || df!("a" => &[1i64], "b" => &[2i64]).unwrap().lazy();
        let schema = std::sync::Arc::new((*lf().collect_schema().unwrap()).clone());
        let state = DataTableState::from_schema_and_lazyframe(
            schema,
            lf(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap();
        let theme = RenderContext::for_test();
        let area = Rect::new(0, 0, 80, 16);
        let footer = |header_toggle: bool, tab: InfoTab| {
            let mut buf = Buffer::empty(area);
            let mut modal = InfoModal {
                active_tab: tab,
                ..Default::default()
            };
            let mut panel = DataTableInfo::new(
                &state,
                InfoContext {
                    format: None,
                    facts: None,
                    facts_tab: None,
                    footer_expected: false,
                    declared_types: false,
                },
                &mut modal,
                &theme,
            );
            panel.header_toggle = header_toggle;
            (&mut panel).render(area, &mut buf);
            crate::tests::buffer_text(&buf)
        };
        let shown = footer(true, InfoTab::Schema);
        assert!(shown.contains("Header"), "{shown}");
        assert!(!footer(false, InfoTab::Schema).contains("Header"));
        assert!(!footer(true, InfoTab::Resources).contains("Header"));
    }

    /// Times read in the unit the docs promise, on both sides of the switch.
    ///
    /// The band just under a second is the whole point. Judging it in whole
    /// milliseconds moves the switch to 999.5 ms, so half a millisecond's worth of
    /// figures the page promises in milliseconds come back in seconds; not rounding at
    /// all prints `1000.00 ms`, which beside the `1.00s` a tick later says the slower
    /// open was the faster one. Neither shows up in a test that only uses round
    /// numbers, which is why these are not round.
    #[test]
    fn a_time_reads_in_the_unit_the_page_promises() {
        use std::time::Duration;

        let cases = [
            (Duration::ZERO, "0.00 ms"),
            (Duration::from_nanos(1_000), "0.00 ms"),
            (Duration::from_nanos(5_000), "0.01 ms"),
            (Duration::from_micros(344), "0.34 ms"),
            (Duration::from_micros(999_500), "999.50 ms"),
            (Duration::from_nanos(999_994_999), "999.99 ms"),
            (Duration::from_nanos(999_995_000), "1.00s"),
            (Duration::from_secs(1), "1.00s"),
            (Duration::from_millis(3_880), "3.88s"),
        ];
        for (took, expected) in cases {
            assert_eq!(
                format_took(took),
                expected,
                "{took:?} should read as {expected}"
            );
        }
    }

    /// The Resources tab says how the open reads the data, and nothing for a frame no
    /// open found.
    #[test]
    fn the_resources_tab_says_how_the_data_is_read() {
        use crate::table::{DataTableState, OpenFacts};
        let painted = |read_mode: Option<crate::ReadMode>| {
            let rows = || df!("id" => [1i64, 2]).unwrap().lazy();
            let schema = Arc::new((*rows().collect_schema().unwrap()).clone());
            let state = DataTableState::from_schema_and_lazyframe(
                schema,
                rows(),
                &crate::OpenOptions::default(),
                None,
            )
            .unwrap()
            .with_open(OpenFacts {
                read_mode,
                ..Default::default()
            });
            let theme = RenderContext::for_test();
            let area = Rect::new(0, 0, 60, 12);
            let mut buf = Buffer::empty(area);
            let mut modal = InfoModal::default();
            DataTableInfo::new(
                &state,
                InfoContext {
                    format: None,
                    facts: None,
                    facts_tab: None,
                    footer_expected: false,
                    declared_types: false,
                },
                &mut modal,
                &theme,
            )
            .render_resources_tab(area, &mut buf);
            crate::tests::buffer_lines(&buf)
        };
        let lines = painted(Some(crate::ReadMode::InMemory));
        assert!(
            lines
                .iter()
                .any(|l| l.trim_end() == format!("{:<17}in memory", "Read:")),
            "{lines:#?}"
        );
        let converted = painted(Some(crate::ReadMode::Converted));
        assert!(converted.iter().any(|l| l.contains("converted to Arrow")));
        assert!(!painted(None).iter().any(|l| l.starts_with("Read:")));
    }

    /// The Resources tab shows what the open cost, and shows only what was measured.
    ///
    /// Through the rendered tab rather than [`measurement_line`], because the bug worth
    /// guarding is a row reaching the panel for a stretch of work that never ran — a
    /// dataset opened before any of this existed would otherwise read as one whose
    /// listing took no time at all.
    #[test]
    fn the_resources_tab_shows_what_was_measured_and_nothing_else() {
        use crate::measurements::Meter;
        use crate::table::DataTableState;
        use polars::prelude::*;
        use std::time::Duration;

        // The meter rides on the dataset, so each case paints a dataset carrying the
        // meter under test rather than handing one to the panel beside it.
        let dataset_with = |meter: &std::sync::Arc<Meter>| {
            let rows = || df!("id" => (0..3i64).collect::<Vec<_>>()).unwrap().lazy();
            let mut lf = rows();
            let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
            DataTableState::from_schema_and_lazyframe(
                schema,
                rows(),
                &crate::OpenOptions::default(),
                None,
            )
            .unwrap()
            .with_open(crate::table::OpenFacts {
                measurements: meter.clone(),
                ..Default::default()
            })
        };

        let theme = RenderContext::for_test();
        let painted = |meter: &std::sync::Arc<Meter>, height: u16| {
            let state = dataset_with(meter);
            let area = Rect::new(0, 0, 70, height);
            let mut buf = Buffer::empty(area);
            let mut modal = InfoModal::default();
            let panel = DataTableInfo::new(
                &state,
                InfoContext {
                    format: None,
                    facts: None,
                    facts_tab: None,
                    footer_expected: false,
                    declared_types: false,
                },
                &mut modal,
                &theme,
            );
            panel.render_resources_tab(area, &mut buf);
            crate::tests::buffer_text(&buf)
        };

        // Nothing measured: no heading, and above all no row of zeroes standing in for
        // a measurement that was never taken.
        let unmeasured = painted(&std::sync::Arc::new(Meter::default()), 24);
        assert!(
            !unmeasured.contains("Measurements"),
            "a meter holding nothing has nothing to show: {unmeasured}"
        );

        // A local open: two stretches, neither of which made a request.
        let local = std::sync::Arc::new(Meter::default());
        local.listed(Duration::from_micros(344), Some(6541), false);
        local.read_footers(Duration::from_millis(3880), Some(6541), false);
        let shown = painted(&local, 24);
        assert!(
            shown.contains("Measurements"),
            "once there is something to say, the section appears: {shown}"
        );
        assert!(
            shown.contains("0.34 ms, 6,541 files"),
            "a listing that really took a third of a millisecond says so, rather than \
             rounding to a figure that reads as unmeasured: {shown}"
        );
        assert!(
            shown.contains("3.88s, 6,541 footers read"),
            "and a stretch over a second is in seconds, counting footers rather than files: {shown}"
        );
        assert!(
            shown.contains("Total:") && shown.contains("3.88s"),
            "the total is a time: {shown}"
        );
        // Every value starts in the one value column, the buffer's too.
        for label in [
            "File size:",
            "Buffer (Rows):",
            "Buffer (MB):",
            "Listing:",
            "Total:",
        ] {
            let line = shown
                .lines()
                .find(|l| l.starts_with(label))
                .unwrap_or_else(|| panic!("{label} in {shown}"));
            let cells: Vec<char> = line.chars().collect();
            assert!(
                cells[label.len()..17].iter().all(|c| *c == ' ') && cells[17] != ' ',
                "{label} value at column 17: {line:?}"
            );
        }
        assert!(
            shown
                .lines()
                .any(|l| l.starts_with("Measurements ") && l.contains(crate::glyphs::get().rule_h)),
            "the heading is a section rule: {shown}"
        );
        assert!(
            !shown.contains("13,082"),
            "and not the two file counts added together, which is not the size of \
             anything: {shown}"
        );
        assert!(
            !shown.contains("requests"),
            "a local open made none, and says nothing rather than saying zero: {shown}"
        );

        // One of a thing is one of a thing. A one-file directory and a single remote
        // object both reach this, and "1 files read" is what the counts are for.
        let just_one = std::sync::Arc::new(Meter::default());
        just_one.listed(Duration::from_millis(1), Some(1), false);
        just_one.footer_request(512);
        just_one.read_footers(Duration::from_millis(2), Some(1), true);
        let singular = painted(&just_one, 24);
        assert!(
            singular.contains("1 file,") || singular.contains("1 file "),
            "one file, not one files: {singular}"
        );
        assert!(
            singular.contains("1 footer read,"),
            "and one footer read, not one footers read: {singular}"
        );
        assert!(
            !singular.contains("1 files") && !singular.contains("1 footers"),
            "neither plural appears anywhere: {singular}"
        );

        // A glob: a listing with no file count and nothing over the wire, beside
        // footers that have both. The row must show a bare time — a `0 files` or a
        // `0 requests` here would each say datui looked and found none.
        let globbed = std::sync::Arc::new(Meter::default());
        globbed.listed(Duration::from_millis(1), None, false);
        // Two footers, two requests each: this route must ask an object's size before
        // it can ask for its tail.
        for _ in 0..4 {
            globbed.footer_request(250);
        }
        globbed.read_footers(Duration::from_millis(3), Some(2), true);
        let glob_shown = painted(&globbed, 24);
        let row = |label: &str| -> String {
            glob_shown
                .lines()
                .find(|l| l.trim_start().starts_with(label))
                .unwrap_or_else(|| panic!("{label} row is shown: {glob_shown}"))
                .to_string()
        };
        let listing_row = row("Listing:");
        assert_eq!(
            listing_row.trim_end(),
            "Listing:         1.00 ms",
            "the walk reports a time and nothing else: no file count it never learned, \
             and no request count no listing route can take"
        );
        let total_row = row("Total:");
        assert!(
            total_row.contains("4.00 ms") && total_row.contains("4 requests"),
            "and the total is both times with the footer reads' requests: {total_row:?}"
        );

        // A remote open: the footer pass counted its own requests and bytes.
        let remote = std::sync::Arc::new(Meter::default());
        remote.listed(Duration::from_millis(500), Some(3), false);
        remote.footer_request(49_152);
        remote.read_footers(Duration::from_millis(1500), Some(3), true);
        let over_wire = painted(&remote, 24);
        assert!(
            over_wire.contains("1.50s, 3 footers read, 1 request, 48.0 KiB"),
            "the footer row says what datui asked for and what came back: {over_wire}"
        );
        assert!(
            over_wire.contains("500.00 ms, 3 files") && !over_wire.contains("500.00 ms, 3 files, "),
            "while the listing, whose pages the store turns over itself, claims no \
             requests of its own: {over_wire}"
        );

        // Every height, down to one that fits nothing. A heading with no row under it
        // is the failure this checks for: it says a section was cut off where there may
        // have been nothing to cut, and there is exactly one height per tab layout at
        // which a guard that is short by one produces it.
        for height in 1..=24u16 {
            let short = painted(&local, height);
            if short.contains("Measurements") {
                assert!(
                    short.contains("Listing:"),
                    "at height {height} the heading is shown with no row under it: {short}"
                );
            }
        }
    }

    /// A file of several tables names the others under the schema's size; a file of
    /// one adds no line.
    #[test]
    fn the_schema_tab_names_a_file_s_other_tables() {
        use crate::table::{DataTableState, OpenFacts};
        use polars::prelude::*;

        let theme = RenderContext::for_test();
        let paint = |other_tables: Vec<String>| {
            let mut lf = df!("id" => &[1i64, 2]).unwrap().lazy();
            let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
            let state = DataTableState::from_schema_and_lazyframe(
                schema,
                lf,
                &crate::OpenOptions::default(),
                None,
            )
            .unwrap()
            .with_open(OpenFacts {
                other_tables,
                ..Default::default()
            });
            let area = Rect::new(0, 0, 70, 12);
            let mut buf = Buffer::empty(area);
            let mut modal = InfoModal::default();
            modal.open();
            let mut panel = DataTableInfo::new(
                &state,
                InfoContext {
                    format: None,
                    facts: None,
                    facts_tab: None,
                    footer_expected: false,
                    declared_types: false,
                },
                &mut modal,
                &theme,
            );
            (&mut panel).render(area, &mut buf);
            crate::tests::buffer_lines(&buf)
        };
        let middot = crate::glyphs::get().middot;
        let text = paint(vec!["GSV 9".into(), "sentences".into()]);
        let line = format!("Other tables (--table): GSV 9 {middot} sentences");
        assert!(text.iter().any(|row| row.contains(&line)), "{text:#?}");
        let text = paint(Vec::new());
        assert!(
            !text.iter().any(|row| row.contains("Other tables")),
            "{text:#?}"
        );
    }

    /// The format tab is named for the format, shows its lines, then its list under a
    /// rule with a count, and says how much of the list is out of view.
    #[test]
    fn the_format_tab_shows_its_lines_and_list() {
        use crate::formats::model_files::MetaValue;
        use crate::formats::text_formats::Detail;
        use crate::table::{DataTableState, OpenFacts};
        use polars::prelude::*;

        let theme = RenderContext::for_test();
        let mut lf = df!("time" => &[1i64]).unwrap().lazy();
        let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
        let list: Vec<(String, MetaValue)> = (0..30)
            .map(|i| {
                (
                    format!("tb.sig{i}"),
                    MetaValue::Text(format!("wire 1 bit id {i}")),
                )
            })
            .collect();
        let state = DataTableState::from_schema_and_lazyframe(
            schema,
            lf,
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap()
        .with_open(OpenFacts {
            detail: Some(std::sync::Arc::new(Detail {
                tab: "VCD",
                lines: vec!["VCD timescale 1ns".into(), "Version: Icarus".into()],
                list_title: "Signals",
                list,
                first: true,
                ..Default::default()
            })),
            ..Default::default()
        });
        let area = Rect::new(0, 0, 60, 20);
        let mut buf = Buffer::empty(area);
        let mut modal = InfoModal::default();
        modal.open_on(InfoTab::Format);
        let mut panel = DataTableInfo::new(
            &state,
            InfoContext {
                format: None,
                facts: None,
                facts_tab: None,
                footer_expected: false,
                declared_types: false,
            },
            &mut modal,
            &theme,
        );
        (&mut panel).render(area, &mut buf);
        let text: Vec<String> = crate::tests::buffer_lines(&buf);
        let has = |needle: &str| text.iter().any(|row| row.contains(needle));
        assert!(has("VCD") && !has("Format"), "{text:#?}");
        assert!(has("Version: Icarus"), "{text:#?}");
        assert!(has("Signals") && has("30"), "{text:#?}");
        assert!(has("tb.sig0") && has("wire 1 bit id 0"), "{text:#?}");
        assert!(has("below"), "the rest is counted: {text:#?}");
    }

    /// The body has the keys: the schema's rule is bright and the row carries the
    /// rail, and the tab line, which never takes focus, has none. One frame, the
    /// footer inside it.
    #[test]
    fn the_body_has_the_accent_and_the_tab_bar_none() {
        use crate::table::DataTableState;
        use polars::prelude::*;

        let rows = || {
            df!("id" => &[1i64, 2], "name" => &["a", "b"])
                .unwrap()
                .lazy()
        };
        let mut lf = rows();
        let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
        let state = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap();
        let theme = RenderContext::for_test();
        let g = crate::glyphs::get();

        let paint = || {
            let area = Rect::new(0, 0, 50, 16);
            let mut buf = Buffer::empty(area);
            let mut modal = InfoModal::default();
            modal.open();
            let mut panel = DataTableInfo::new(
                &state,
                InfoContext {
                    format: None,
                    facts: None,
                    facts_tab: None,
                    footer_expected: false,
                    declared_types: false,
                },
                &mut modal,
                &theme,
            );
            (&mut panel).render(area, &mut buf);
            let text: Vec<String> = crate::tests::buffer_lines(&buf);
            (buf, text)
        };
        let find = |text: &[String], needle: &str| {
            let y = text
                .iter()
                .position(|row| row.contains(needle))
                .unwrap_or_else(|| panic!("{needle:?} not drawn: {text:#?}"));
            // Cells, not bytes: the frame and the rail are multibyte.
            let x = text[y][..text[y].find(needle).unwrap()].chars().count();
            (x as u16, y as u16)
        };

        let (buf, text) = paint();
        let (x, y) = find(&text, "Schema  types inferred");
        // The rule's title in the plain accent: the rail marks focus, not the rule.
        assert_eq!(buf[(x, y)].fg, theme.accent, "{text:#?}");
        let (_, id_row) = find(&text, " id ");
        assert!(text[id_row as usize].contains(g.rail), "{text:#?}");
        let (x, y) = find(&text, "Resources");
        assert!(!text[y as usize].contains(g.rail), "{text:#?}");
        assert_ne!(
            buf[(x, y)].fg,
            theme.accent,
            "an inactive tab is not accented"
        );

        // One frame: its corners on the first and last rows and nowhere else,
        // the footer on the last row inside it.
        for row in &text[1..text.len() - 1] {
            assert!(
                !row.contains(g.border.top_left) && !row.contains(g.border.bottom_left),
                "a second border inside the panel: {text:#?}"
            );
        }
        assert!(text[text.len() - 2].contains("Esc"), "{text:#?}");
    }

    #[test]
    fn the_tabs_on_offer_depend_on_the_dataset() {
        assert_eq!(
            InfoTab::visible(offer(false, false)),
            [InfoTab::Schema, InfoTab::Resources]
        );
        assert_eq!(
            InfoTab::visible(offer(true, true)),
            [
                InfoTab::Schema,
                InfoTab::Resources,
                InfoTab::Partitions,
                InfoTab::Notes
            ]
        );
        assert_eq!(
            InfoTab::visible(offer(false, true)),
            [InfoTab::Schema, InfoTab::Resources, InfoTab::Notes],
            "notes without partitions still sit last"
        );
    }

    #[test]
    fn the_format_tab_sits_beside_the_schema() {
        let offered = TabsOffered {
            format: true,
            ..offer(false, true)
        };
        assert_eq!(
            InfoTab::visible(offered),
            [
                InfoTab::Schema,
                InfoTab::Format,
                InfoTab::Resources,
                InfoTab::Notes
            ]
        );
        assert_eq!(InfoTab::Format.prev(offered), InfoTab::Schema);
        assert_eq!(InfoTab::Format.index(offer(false, false)), 0, "not offered");
    }

    #[test]
    fn a_clock_shows_hours_only_when_there_are_some() {
        assert_eq!(clock(3.25), "0:03.250");
        assert_eq!(clock(62.0), "1:02.000");
        assert_eq!(clock(3723.0005), "1:02:03.001");
    }

    /// A value breaks between words; indentation stays, the spaces at a break go, and
    /// only a word wider than the room is split.
    #[test]
    fn metadata_values_wrap_on_word_boundaries() {
        assert_eq!(
            wrap_words("Broadcast WAV coding history", 12),
            ["Broadcast", "WAV coding", "history"]
        );
        assert_eq!(
            wrap_words("    {% if x %}   y", 10),
            ["    {% if", "x %}   y"]
        );
        assert_eq!(
            wrap_words("a 0123456789abcdef", 6),
            ["a 0123", "456789", "abcdef"]
        );
        // Measured in columns: three double-width characters are six.
        assert_eq!(wrap_words("日本語 text", 7), ["日本語", "text"]);
        assert_eq!(wrap_words("", 5), [""]);
        // Spaces at a break or past the room leave no blank line.
        assert_eq!(wrap_words("abc   ", 4), ["abc"]);
        assert_eq!(wrap_words("          x", 5), ["x"]);
    }

    /// Each value is drawn whole: its own newlines kept, wrapped under the key, a short
    /// array listed and a long one counted.
    #[test]
    fn metadata_values_wrap_whole_under_their_key() {
        use crate::formats::model_files::MetaValue;
        let meta = vec![
            (
                "a".to_string(),
                MetaValue::Text("line one\nsecond line that is long".to_string()),
            ),
            (
                "tokens".to_string(),
                MetaValue::List {
                    of: "strings",
                    len: 151_936,
                    items: vec![],
                },
            ),
            (
                "tags".to_string(),
                MetaValue::List {
                    of: "strings",
                    len: 2,
                    items: vec!["x".to_string(), "y".to_string()],
                },
            ),
        ];
        let lines = metadata_lines(&meta, 20);
        let key = |s: &str| format!("{s:<6}  ");
        let blank = " ".repeat(8);
        assert_eq!(
            lines,
            [
                (key("a"), "line one".to_string()),
                (blank.clone(), "second line".to_string()),
                (blank.clone(), "that is long".to_string()),
                (key("tokens"), "[151,936".to_string()),
                (blank.clone(), "strings]".to_string()),
                (key("tags"), "[\"x\", \"y\"]".to_string()),
            ]
        );
        // A value of megabytes is drawn to its first 64 KiB, and says what is left.
        let huge = vec![(
            "tokenizer.huggingface.json".to_string(),
            MetaValue::Text("x".repeat(VALUE_SHOWN_BYTES + 2048)),
        )];
        let lines = metadata_lines(&huge, 80);
        let last = &lines.last().unwrap().1;
        assert!(last.ends_with("2.0 KiB more"), "{last}");
        let drawn: usize = lines[..lines.len() - 1].iter().map(|(_, v)| v.len()).sum();
        assert_eq!(drawn, VALUE_SHOWN_BYTES);
        assert_eq!(short_count(8_030_261_248), "8.0B");
        assert_eq!(short_count(950), "950");
    }

    #[test]
    fn tab_navigation_wraps_through_what_is_on_offer() {
        // Nothing optional: two tabs, back and forth.
        assert_eq!(
            InfoTab::Schema.next(offer(false, false)),
            InfoTab::Resources
        );
        assert_eq!(
            InfoTab::Resources.next(offer(false, false)),
            InfoTab::Schema
        );
        assert_eq!(
            InfoTab::Schema.prev(offer(false, false)),
            InfoTab::Resources
        );

        // Both optional tabs present.
        assert_eq!(
            InfoTab::Resources.next(offer(true, true)),
            InfoTab::Partitions
        );
        assert_eq!(InfoTab::Partitions.next(offer(true, true)), InfoTab::Notes);
        assert_eq!(InfoTab::Notes.next(offer(true, true)), InfoTab::Schema);
        assert_eq!(InfoTab::Schema.prev(offer(true, true)), InfoTab::Notes);

        // Notes only.
        assert_eq!(InfoTab::Resources.next(offer(false, true)), InfoTab::Notes);
        assert_eq!(InfoTab::Notes.prev(offer(false, true)), InfoTab::Resources);
    }

    /// A tab that is no longer on offer must not strand the cursor: it reads as the
    /// first tab, so moving on from it goes somewhere real.
    #[test]
    fn a_tab_that_is_no_longer_offered_falls_back_to_the_first() {
        assert_eq!(InfoTab::Notes.index(offer(false, false)), 0);
        assert_eq!(InfoTab::Notes.next(offer(false, false)), InfoTab::Resources);
        assert_eq!(InfoTab::Partitions.index(offer(false, false)), 0);
        assert_eq!(
            InfoTab::Partitions.prev(offer(false, false)),
            InfoTab::Resources
        );
    }

    /// The window's three promises, checked over every shape that fits in a terminal.
    ///
    /// Review after review found defects in this arithmetic while it lived inside the
    /// render, and the test that was meant to guard it re-implemented the same
    /// arithmetic — so the two drifted and it could never fail. This calls the real
    /// function and asserts what the panel actually needs.
    #[test]
    fn the_notes_window_always_shows_the_selected_note_and_wastes_no_room() {
        let shapes: Vec<Vec<usize>> = vec![
            vec![2, 2, 2, 2, 2, 2],
            vec![2],
            vec![3, 2, 4, 2],
            vec![2, 9, 2],
            vec![5, 5, 5],
            vec![1, 1, 1, 1, 1, 1, 1, 1],
            vec![4, 2, 2, 7, 2],
        ];
        let span = |h: &[usize], a: usize, b: usize| {
            h[a..b].iter().sum::<usize>() + (b - a).saturating_sub(1)
        };
        for heights in &shapes {
            for show in 1..=30usize {
                for selected in 0..heights.len() {
                    for stored in 0..heights.len() {
                        let (first, last) = notes_window(heights, selected, stored, show);
                        let at = format!(
                            "heights {heights:?}, show {show}, selected {selected}, stored {stored}"
                        );

                        assert!(first <= selected, "the cursor is above the window at {at}");
                        assert!(selected < last, "the cursor is below the window at {at}");

                        let used = span(heights, first, last);
                        if last - first > 1 {
                            assert!(used <= show, "{used} rows in {show} at {at}");
                        }

                        // Nothing more would fit below, and nothing more would fit above.
                        if last < heights.len() {
                            assert!(
                                span(heights, first, last + 1) > show,
                                "another note below would have fitted at {at}"
                            );
                        }
                        if first > 0 {
                            assert!(
                                span(heights, first - 1, last) > show,
                                "another note above would have fitted at {at}"
                            );
                        }
                    }
                }
            }
        }
    }

    /// A note taller than the whole panel is still drawn, because leaving it out would
    /// put it out of reach.
    #[test]
    fn a_note_taller_than_the_panel_is_still_the_window() {
        let (first, last) = notes_window(&[2, 9, 2], 1, 0, 4);
        assert_eq!((first, last), (1, 2), "just the note that does not fit");
    }

    /// The panel is the only thing that decides how many notes fit, and the cursor can
    /// always reach the last of them.
    #[test]
    fn the_notes_cursor_reaches_every_note() {
        let mut modal = InfoModal::new();
        assert!(!modal.notes_move(1, 0), "nothing to move through");
        for expected in 1..5 {
            assert!(modal.notes_move(1, 5));
            assert_eq!(modal.notes_selected_index, expected);
        }
        assert!(!modal.notes_move(1, 5), "and stops at the last");
        for expected in (0..4).rev() {
            assert!(modal.notes_move(-1, 5));
            assert_eq!(modal.notes_selected_index, expected);
        }
        assert!(!modal.notes_move(-1, 5), "and at the first");
    }

    #[test]
    fn wrapping_measures_columns_not_characters() {
        assert_eq!(wrap_to("one two three", 9), ["one two", "three"]);
        assert_eq!(wrap_to("", 10), [""], "an empty line is still a line");
        assert_eq!(
            wrap_to("supercalifragilistic", 5),
            ["supercalifragilistic"],
            "a word longer than the panel is left whole rather than broken"
        );
        // Double-width characters take two columns each, so four of them fill eight.
        assert_eq!(wrap_to("日本語表 x", 8), ["日本語表", "x"]);
    }

    #[test]
    fn test_format_int() {
        assert_eq!(format_int(0), "0");
        assert_eq!(format_int(1234), "1,234");
        assert_eq!(format_int(1_234_567), "1,234,567");
    }
}
