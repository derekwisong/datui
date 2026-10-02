//! Info panel: tabbed Schema and Resources view for dataset technical info.

use std::collections::HashMap;

use crate::numfmt::group_chrome;
use std::path::Path;
use std::sync::Arc;

use polars::prelude::*;
use polars_parquet::parquet::metadata::FileMetadata;
use polars_parquet::parquet::read::read_metadata;

/// Type alias for cached Parquet metadata (used by App).
pub type ParquetMetadataCache = Arc<FileMetadata>;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::Stylize;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Gauge, HighlightSpacing, Paragraph, Row, StatefulWidget, Table, Widget};

use super::datatable::DataTableState;
use crate::export_modal::ExportFormat;
use crate::render::context::RenderContext;
use crate::widgets::ui::{HintBar, SectionRule, Surface};

/// One drawn line of the Notes tab.
struct NoteRow {
    text: String,
    /// Drawn in the panel's dim color: the line a note rests on, not the note.
    dim: bool,
}

/// Which notes to draw, as a half-open range.
///
/// `heights` is each note's rows; a blank line sits between adjacent notes. `stored` is
/// where the panel was last scrolled to, and `show` the rows available.
///
/// Guarantees, whatever it is given:
///
/// - `first <= selected < last`, so the note the cursor is on is always drawn;
/// - the notes in the range fit `show` rows, unless the range is one note that does not
///   fit on its own — then it is drawn as far as it goes, since leaving it out would
///   make it unreachable;
/// - `last` is as large as it can be, so rows are never left blank while a whole note
///   is out of view.
///
/// Review after review found defects in this arithmetic when it was inline in the
/// render, expressed in rows and mixed with drawing. It is a function so the rules
/// above can be checked directly rather than through a terminal.
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

/// Break `text` on spaces so no line runs past `width` columns.
///
/// Measured in columns rather than characters: a column name can be any text the data
/// holds, and a name whose characters are double-width would otherwise be clipped by
/// the terminal after this said it fitted. A single word longer than the panel is left
/// whole rather than split mid-word, though the terminal still clips what runs past
/// the edge — the wrapping protects the note's height, not a single long word.
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
fn group_u64(n: u64) -> String {
    let mut out = String::new();
    crate::numfmt::NumberFormat::CHROME.write_u64(n, &mut out);
    out
}

/// `n` and the noun for it: `1 tensor`, `291 tensors`.
fn count_of(n: u64, one: &str, many: &str) -> String {
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

/// Each type's share of the parameters, most first: `Q4_K 87% · Q6_K 12% · F32 <1%`.
/// By tensors when no tensor has a parameter count.
fn type_mix(types: &[crate::model_files::TypeShare], sep: &str) -> String {
    let by_params = types.iter().any(|t| t.params > 0);
    let total: u64 = if by_params {
        types.iter().map(|t| t.params).fold(0, u64::saturating_add)
    } else {
        types.iter().map(|t| t.tensors as u64).sum()
    };
    types
        .iter()
        .map(|t| {
            let part = if by_params {
                t.params
            } else {
                t.tensors as u64
            };
            let pct = if total == 0 {
                0.0
            } else {
                part as f64 * 100.0 / total as f64
            };
            if pct > 0.0 && pct < 1.0 {
                format!("{} <1%", t.name)
            } else {
                format!("{} {:.0}%", t.name, pct)
            }
        })
        .collect::<Vec<_>>()
        .join(sep)
}

/// One metadata value as text: an array that was listed, or how long it is.
pub(crate) fn meta_text(value: &crate::model_files::MetaValue) -> String {
    use crate::model_files::MetaValue;
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

/// The most of one metadata value the Model tab draws. A chat template is a few KB and
/// is shown whole; a GGUF can carry a whole `tokenizer.json` as one string, megabytes
/// that would be wrapped again on every frame.
pub(crate) const VALUE_SHOWN_BYTES: usize = 64 * 1024;

/// The metadata as drawn lines: the key on a value's first line, blank under it, and
/// each value cut at its own newlines and wrapped to what is left of `width`. A value
/// past [`VALUE_SHOWN_BYTES`] ends with a line saying how much more there is.
pub(crate) fn metadata_lines(
    metadata: &[(String, crate::model_files::MetaValue)],
    width: usize,
) -> Vec<(String, String)> {
    use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};
    let longest = metadata.iter().map(|(k, _)| k.width()).max().unwrap_or(0);
    // Two columns between key and value; the key takes no more than two fifths.
    let key_width = longest.min(width * 2 / 5).max(1);
    let value_width = width.saturating_sub(key_width + 2).max(1);
    let mut out = Vec::new();
    for (key, value) in metadata {
        let key_cell = format!("{:<w$}  ", clip(key, key_width), w = key_width);
        let blank = " ".repeat(key_width + 2);
        // Borrowed, not copied: this runs every frame.
        let listed;
        let text = match value {
            crate::model_files::MetaValue::Text(text) => text.as_str(),
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
            let mut line = String::new();
            let mut line_width = 0;
            let mut push = |line: &mut String, out: &mut Vec<(String, String)>| {
                let k = if first {
                    key_cell.clone()
                } else {
                    blank.clone()
                };
                first = false;
                out.push((k, std::mem::take(line)));
            };
            for c in clean.chars() {
                let w = c.width().unwrap_or(0);
                if line_width + w > value_width && !line.is_empty() {
                    push(&mut line, &mut out);
                    line_width = 0;
                }
                line.push(c);
                line_width += w;
            }
            push(&mut line, &mut out);
        }
        if more > 0 {
            let g = crate::glyphs::get();
            out.push((
                blank,
                format!("{} {} more", g.ellipsis, format_bytes(more as u64)),
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

/// A detail tab's lines above its metadata, each with its style.
pub(crate) type HeadLines = Vec<(String, Style)>;

/// The Audio tab: its head lines and its metadata, markers last.
pub(crate) fn audio_detail(
    audio: &crate::audio::AudioSource,
    theme: &RenderContext,
) -> (HeadLines, crate::model_files::Metadata) {
    use crate::model_files::MetaValue;
    let h = audio.header();
    let g = crate::glyphs::get();
    let sep = format!(" {} ", g.middot);
    let plain = Style::default();
    let mut kind = h.container.label().to_string();
    if h.broadcast {
        kind.push_str(" (Broadcast WAV)");
    }
    let rate = if h.sample_rate.fract() == 0.0 {
        group_u64(h.sample_rate as u64)
    } else {
        format!("{:.3}", h.sample_rate)
    };
    let mut samples = h.sample.label();
    if !h.sample.is_float() && (h.valid_bits as usize) < h.sample.bytes() * 8 {
        samples.push_str(&format!(" ({} valid)", h.valid_bits));
    }
    let mut lines = vec![
        (
            format!(
                "{kind}{sep}{}{sep}{rate} Hz",
                count_of(h.channels as u64, "channel", "channels"),
            ),
            plain,
        ),
        (
            format!(
                "Samples: {samples}{sep}{}{}",
                h.encoding,
                if audio.normalize() && !h.sample.is_float() {
                    format!("{sep}shown as float in [-1, 1]")
                } else {
                    String::new()
                }
            ),
            plain,
        ),
        (
            format!(
                "Frames: {}{sep}Length: {}",
                group_u64(audio.frames()),
                clock(audio.seconds()),
            ),
            plain,
        ),
        (
            format!(
                "Data: {}",
                format_bytes(audio.frames() * h.frame_bytes as u64)
            ),
            plain,
        ),
    ];
    let warn = Style::default().fg(theme.warning);
    if let Some((declared, held)) = audio.cut_short() {
        lines.push((
            format!(
                "The header says {} of samples; the file holds {}",
                format_bytes(declared),
                format_bytes(held)
            ),
            warn,
        ));
    } else if h.data_declared.is_none() && audio.frames() > 0 {
        lines.push((
            "No data size in the header; frames are counted from the file's size".into(),
            plain,
        ));
    }
    let trailing = audio.trailing_bytes();
    if trailing > 0 {
        lines.push((
            format!("{trailing} bytes after the last whole frame are not shown"),
            warn,
        ));
    }
    let mut metadata: crate::model_files::Metadata = h
        .metadata
        .iter()
        .map(|(k, v)| (k.clone(), MetaValue::Text(v.clone())))
        .collect();
    for marker in &h.markers {
        let mut value = format!(
            "{}{sep}frame {}",
            clock(marker.sample as f64 / h.sample_rate),
            group_u64(marker.sample)
        );
        if let Some(length) = marker.length {
            value.push_str(&format!(
                "{sep}{} long",
                clock(length as f64 / h.sample_rate)
            ));
        }
        if !marker.label.is_empty() {
            value.push_str(&sep);
            value.push_str(&marker.label);
        }
        metadata.push((format!("marker {}", marker.id), MetaValue::Text(value)));
    }
    (lines, metadata)
}

/// Human-readable byte size (e.g. "1.2 MiB", "456 KiB").
pub fn format_bytes(n: u64) -> String {
    const K: u64 = 1024;
    const M: u64 = K * K;
    const G: u64 = M * K;
    if n >= G {
        format!("{:.1} GiB", n as f64 / G as f64)
    } else if n >= M {
        format!("{:.1} MiB", n as f64 / M as f64)
    } else if n >= K {
        format!("{:.1} KiB", n as f64 / K as f64)
    } else {
        format!("{} B", n)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InfoTab {
    #[default]
    Schema,
    /// A SafeTensors or GGUF model's totals and metadata.
    Model,
    /// An audio file's format, length and metadata.
    Audio,
    Resources,
    Partitions,
    Notes,
}

/// Which of the optional tabs the dataset on screen offers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TabsOffered {
    /// The dataset is a model file's tensors.
    pub model: bool,
    /// The dataset is an audio file's frames.
    pub audio: bool,
    pub partitions: bool,
    pub notes: bool,
}

impl TabsOffered {
    /// What `state` offers.
    pub fn of(state: &DataTableState) -> Self {
        Self {
            model: state.model().is_some(),
            audio: state.audio().is_some(),
            partitions: state
                .partition_columns()
                .map(|v| !v.is_empty())
                .unwrap_or(false),
            notes: state.has_notes(),
        }
    }
}

impl InfoTab {
    /// The tabs on offer, in order: Model only for a model file and Audio only for an
    /// audio file, beside the schema they explain; Partitions only for a partitioned
    /// dataset; Notes only when datui has something to say about the data.
    pub fn visible(offered: TabsOffered) -> Vec<InfoTab> {
        let mut tabs = vec![InfoTab::Schema];
        if offered.model {
            tabs.push(InfoTab::Model);
        }
        if offered.audio {
            tabs.push(InfoTab::Audio);
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
            InfoTab::Model => "Model",
            InfoTab::Audio => "Audio",
            InfoTab::Resources => "Resources",
            InfoTab::Partitions => "Partitions",
            InfoTab::Notes => "Notes",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InfoFocus {
    #[default]
    TabBar,
    Body,
}

/// Modal state for the Info panel: focus, tab, schema table selection/scroll.
#[derive(Default)]
pub struct InfoModal {
    pub active: bool,
    pub active_tab: InfoTab,
    pub focus: InfoFocus,
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
    /// The first line of the Model or Audio tab's metadata on screen. The render clamps
    /// it. One for both: a dataset is never a model and audio at once.
    pub detail_scroll: usize,
    /// The metadata lines the Model or Audio tab last had room for; set during render.
    pub detail_visible: usize,
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
        self.active = true;
        self.active_tab = tab;
        self.focus = InfoFocus::Body;
        self.schema_selected_index = 0;
        self.schema_scroll_offset = 0;
        self.schema_table_state.select(Some(0));
        self.notes_selected_index = 0;
        self.notes_scroll_offset = 0;
        self.detail_scroll = 0;
    }

    pub fn close(&mut self) {
        self.active = false;
    }

    pub fn next_focus(&mut self) {
        self.focus = match self.focus {
            InfoFocus::TabBar => InfoFocus::Body,
            InfoFocus::Body => InfoFocus::TabBar,
        };
    }

    pub fn prev_focus(&mut self) {
        self.focus = match self.focus {
            InfoFocus::TabBar => InfoFocus::Body,
            InfoFocus::Body => InfoFocus::TabBar,
        };
    }

    /// Switch to the next of the tabs on offer.
    pub fn switch_tab(&mut self, offered: TabsOffered) {
        self.active_tab = self.active_tab.next(offered);
        if self.active_tab == InfoTab::Schema {
            self.schema_selected_index = 0;
            self.schema_scroll_offset = 0;
            self.schema_table_state.select(Some(0));
        } else {
            self.focus = InfoFocus::TabBar;
        }
    }

    /// Switch to the previous of the tabs on offer.
    pub fn switch_tab_prev(&mut self, offered: TabsOffered) {
        self.active_tab = self.active_tab.prev(offered);
        if self.active_tab == InfoTab::Schema {
            self.schema_selected_index = 0;
            self.schema_scroll_offset = 0;
            self.schema_table_state.select(Some(0));
        } else {
            self.focus = InfoFocus::TabBar;
        }
    }

    /// Scroll the Model or Audio tab's metadata by `delta` lines; the render keeps it
    /// in range.
    pub fn detail_scroll_by(&mut self, delta: isize) {
        self.detail_scroll = self.detail_scroll.saturating_add_signed(delta);
    }

    /// Scroll the Model or Audio tab's metadata by a page.
    pub fn detail_page(&mut self, down: bool) {
        let page = self.detail_visible.max(1) as isize;
        self.detail_scroll_by(if down { page } else { -page });
    }

    /// Move the cursor through the notes. Returns true when something changed.
    ///
    /// Only the index moves: the render scrolls to whatever is selected, so how tall a
    /// note happens to be can never decide how far the cursor may go.
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

/// What the open file says about itself beyond its rows: its size on disk and, for a
/// Parquet file, its footer.
///
/// Read on a worker, once per dataset, and drawn from here. A stat or a footer read on a
/// mount that has stopped answering hangs the thread that makes it, so neither is made
/// where keys are read or frames drawn (#457).
#[derive(Debug, Clone)]
pub enum FileFacts {
    /// Asked for; the worker has not answered.
    Reading,
    /// What the worker found.
    Read {
        /// `None` for a directory, whose own size is not the data's.
        size: Option<u64>,
        /// The footer of a Parquet file; `None` for any other format.
        parquet: Option<ParquetMetadataCache>,
    },
    /// The read failed, and why. Kept for the dataset rather than asked again: a file
    /// that could not be read a moment ago is not worth a read per frame.
    Failed(String),
}

impl FileFacts {
    /// Stat `path` and, when `parquet`, read its footer. Blocking: call it on a worker.
    ///
    /// The reason for a failure is short enough for the panel's one line; the whole
    /// error goes to the log.
    pub fn read(path: &Path, parquet: bool) -> std::result::Result<Self, String> {
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
                parquet: None,
            });
        }
        let parquet = if parquet {
            let mut file = std::fs::File::open(path).map_err(io)?;
            let footer = read_metadata(&mut file).map_err(|e| {
                log::warn!(target: "datui", "Parquet footer of {}: {e}", path.display());
                "unreadable Parquet footer".to_string()
            })?;
            Some(Arc::new(footer))
        } else {
            None
        };
        Ok(Self::Read {
            size: Some(meta.len()),
            parquet,
        })
    }
}

/// `text` cut to `room` columns, with the ellipsis glyph saying where.
fn clip(text: &str, room: usize) -> String {
    use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};
    if text.width() <= room {
        return text.to_string();
    }
    let mark = crate::glyphs::get().ellipsis;
    let mut kept = String::new();
    for ch in text.chars() {
        if kept.width() + ch.width().unwrap_or(0) + mark.width() > room {
            break;
        }
        kept.push(ch);
    }
    kept + mark
}

/// Context for the info panel: the format, and what the file says about itself.
///
/// What the open cost is not here: it belongs to the dataset, and the panel already
/// has the dataset.
pub struct InfoContext<'a> {
    pub format: Option<ExportFormat>,
    /// `None` when there is no one file on this machine to ask: a remote source, a
    /// glob, or a dataset opened from several paths.
    pub facts: Option<&'a FileFacts>,
    /// The facts are one Parquet file's, so its footer's rows and Compression column
    /// keep their room while it is read: nothing moves when it lands.
    pub parquet_file: bool,
}

impl<'a> InfoContext<'a> {
    pub fn schema_source(&self) -> &'static str {
        match self.format {
            Some(ExportFormat::Parquet) | Some(ExportFormat::Ipc) | Some(ExportFormat::Avro) => {
                "Known"
            }
            _ => "Inferred",
        }
    }

    /// The Parquet footer, once it has been read.
    pub fn parquet_metadata(&self) -> Option<&'a ParquetMetadataCache> {
        match self.facts? {
            FileFacts::Read { parquet, .. } => parquet.as_ref(),
            FileFacts::Reading | FileFacts::Failed(_) => None,
        }
    }

    /// Whether the worker has yet to answer.
    fn reading(&self) -> bool {
        matches!(self.facts, Some(FileFacts::Reading))
    }
}

/// Per-column compression info (Parquet): codec name and ratio.
fn parquet_column_compression(
    meta: &FileMetadata,
    polars_schema: &Schema,
) -> HashMap<String, (String, f64)> {
    let mut by_name: HashMap<String, (u64, u64)> = HashMap::new();
    let mut codec_by_name: HashMap<String, String> = HashMap::new();
    for rg in &meta.row_groups {
        for cc in rg.parquet_columns() {
            let name = cc
                .descriptor()
                .path_in_schema
                .first()
                .map(|s| s.as_ref())
                .unwrap_or("");
            let comp = cc.compressed_size() as u64;
            let uncomp = cc.uncompressed_size() as u64;
            let codec = format!("{:?}", cc.compression()).to_lowercase();
            let e = by_name.entry(name.to_string()).or_insert((0, 0));
            e.0 = e.0.saturating_add(comp);
            e.1 = e.1.saturating_add(uncomp);
            codec_by_name.insert(name.to_string(), codec);
        }
    }
    let mut out = HashMap::new();
    for (name, (comp, uncomp)) in by_name {
        if !polars_schema.contains(&name) {
            continue;
        }
        let codec = codec_by_name
            .get(&name)
            .cloned()
            .unwrap_or_else(|| crate::glyphs::get().dash.to_string());
        if comp > 0 && uncomp > 0 {
            let ratio = uncomp as f64 / comp as f64;
            out.insert(name, (codec, ratio));
        }
    }
    out
}

/// Overall Parquet compression: (compressed, uncompressed) from row groups.
fn parquet_overall_sizes(meta: &FileMetadata) -> (u64, u64) {
    let mut comp: u64 = 0;
    let mut uncomp: u64 = 0;
    for rg in &meta.row_groups {
        comp = comp.saturating_add(rg.compressed_size() as u64);
        uncomp = uncomp.saturating_add(rg.total_byte_size() as u64);
    }
    (comp, uncomp)
}

pub struct DataTableInfo<'a> {
    pub state: &'a DataTableState,
    pub ctx: InfoContext<'a>,
    pub modal: &'a mut InfoModal,
    pub theme: &'a RenderContext,
}

/// The first line of the Schema tab: the dataset's size, or that it does not know yet.
///
/// Told `None` rather than a number, because what a state holds before it has been
/// counted is how far its buffer reached — printed under a heading that says "total",
/// that reads as the size of the dataset. On a directory of thousands of files still
/// being counted it would say `Rows (total): 70` beside a control bar showing a spinner.
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
        }
    }

    fn render_schema_tab(&mut self, area: Rect, buf: &mut Buffer) {
        let summary = self.render_schema_summary(area, buf);
        let rest = Rect {
            y: area.y + summary,
            height: area.height.saturating_sub(summary),
            ..area
        };
        if rest.height == 0 {
            return;
        }
        self.render_schema_table(rest, buf);
    }

    fn render_schema_summary(&self, area: Rect, buf: &mut Buffer) -> u16 {
        let ncols = self.state.schema().len();
        let mut lines = vec![];
        // `num_rows_if_valid`, not `num_rows`: see `rows_and_columns`.
        lines.push(rows_and_columns(self.state.num_rows_if_valid(), ncols));
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

    fn render_schema_table(&mut self, area: Rect, buf: &mut Buffer) {
        // A dataset of many files says which footers its columns came from; one file
        // says only whether its format declared them.
        let dataset = self.state.dataset_schema();
        let src = match dataset {
            Some(dataset) => dataset.origin.to_string(),
            // A model's or an audio file's columns are datui's own, from the header.
            None if self.state.model().is_some() || self.state.audio().is_some() => {
                "Known".to_string()
            }
            None => self.ctx.schema_source().to_string(),
        };
        // Per column, how many of the footers read carry it. Only a dataset of files
        // can vary this per column; for a single file the dataset-level fact already
        // sits in the block title, so no column repeats it.
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
        let compression = self
            .ctx
            .parquet_metadata()
            .map(|m| parquet_column_compression(m.as_ref(), self.state.schema().as_ref()));
        // Kept for a Parquet file whose footer is still out, so the columns do not
        // re-proportion when it lands.
        let has_comp = self.ctx.parquet_file || compression.as_ref().is_some_and(|c| !c.is_empty());
        let mut header_cells = vec!["Column", "Type"];
        if has_files {
            header_cells.push("Files");
        }
        if has_comp {
            header_cells.push("Compression");
        }
        let header = Row::new(header_cells).bold();

        let total_rows = self.state.schema().len();
        // Focus is the accent on the section rule, and the rail on the row.
        let body_focused = self.modal.focus == InfoFocus::Body;
        let title = format!("Schema: {src}");
        SectionRule {
            title: &title,
            chip: None,
            focused: body_focused,
        }
        .render(Rect { height: 1, ..area }, buf, self.theme);
        let inner = Rect {
            y: area.y + 1,
            height: area.height.saturating_sub(1),
            ..area
        };
        let visible_height = inner.height as usize;

        // One row of header; one more reserved for the out-of-view count when
        // the columns do not all fit, so their existence is stated before any
        // scrolling ("… 3 more" beats half a schema presented as whole).
        let fits = total_rows <= visible_height.saturating_sub(1);
        let data_height = visible_height.saturating_sub(1 + usize::from(!fits));
        self.modal.schema_visible_height = data_height;
        self.modal.sync_schema_table_state(total_rows, data_height);

        let offset = self.modal.schema_scroll_offset;
        let take = data_height.min(total_rows.saturating_sub(offset));
        let mut rows = vec![];
        for (idx, (name, dtype)) in self.state.schema().iter().enumerate() {
            if idx < offset {
                continue;
            }
            if idx >= offset + take {
                break;
            }
            let name_str: &str = name.as_ref();
            let mut cells = vec![name.to_string(), dtype.to_string()];
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
            rows.push(Row::new(cells));
        }

        let widths: Vec<Constraint> = match (has_files, has_comp) {
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
        };
        // The rail and the tint while the table has focus; the accent alone when
        // it does not, so the cursor stays visible without claiming focus. The
        // rail's column is kept either way, so focus arriving moves nothing.
        let g = crate::glyphs::get();
        let (highlight, symbol) = if body_focused {
            (self.theme.highlight_style(), g.selector)
        } else {
            (Style::default().fg(self.theme.accent), g.selector_blank)
        };
        let symbol = Span::styled(symbol, Style::default().fg(self.theme.accent));
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
    }

    fn render_resources_tab(&self, area: Rect, buf: &mut Buffer) {
        // One past the longest label, "Parquet version:", so no value touches its label.
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
            }) => Span::raw(format_bytes(*size)),
            Some(FileFacts::Reading) => {
                Span::styled("reading...", Style::default().fg(self.theme.dimmed))
            }
            Some(FileFacts::Failed(why)) => Span::styled(
                clip(why, size_chunks[1].width as usize),
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
        if max_rows > 0 {
            let ratio = (buf_rows as f64 / max_rows as f64).min(1.0);
            let label = format!("{} / {}", format_int(buf_rows), format_int(max_rows));
            Gauge::default()
                .gauge_style(Style::default().fg(self.theme.text_primary))
                .ratio(ratio)
                .label(Span::raw(label))
                .render(row_chunks[1], buf);
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
            let current_mb = buf_mb.unwrap_or(0);
            let ratio = (current_mb as f64 / max_mb as f64).min(1.0);
            let label = match buf_mb {
                Some(m) => format!("{:.1} / {} MiB", m as f64, max_mb),
                None => crate::glyphs::get().dash.to_string(),
            };
            Gauge::default()
                .gauge_style(Style::default().fg(self.theme.text_primary))
                .ratio(ratio)
                .label(Span::raw(label))
                .render(mb_chunks[1], buf);
        } else {
            let value = buf_mb
                .map(|m| format!("{:.1} MiB", m as f64))
                .unwrap_or_else(|| {
                    self.state
                        .buffered_memory_bytes()
                        .map(|b| format_bytes(b as u64))
                        .unwrap_or_else(|| crate::glyphs::get().dash.to_string())
                });
            Paragraph::new(value).render(mb_chunks[1], buf);
        }
        y += 1;

        // Always four rows, laid out before the footer lands, so Measurements below
        // stays put; a value the footer does not have is a dash.
        let meta = self.ctx.parquet_metadata();
        if meta.is_some() || self.ctx.parquet_file {
            let dash = crate::glyphs::get().dash;
            let value = |known: Option<String>| match known {
                Some(v) => v,
                None if self.ctx.reading() => String::new(),
                None => dash.to_string(),
            };
            let comp = meta.and_then(|meta| {
                let (comp, uncomp) = parquet_overall_sizes(meta.as_ref());
                (comp > 0 && uncomp > 0).then(|| {
                    format!(
                        "{:.1}{} (uncomp. {})",
                        uncomp as f64 / comp as f64,
                        crate::glyphs::get().times,
                        format_bytes(uncomp)
                    )
                })
            });
            let rows = [
                ("Parquet comp.:", value(comp)),
                (
                    "Row groups:",
                    value(meta.map(|m| m.row_groups.len().to_string())),
                ),
                (
                    "Parquet version:",
                    value(meta.map(|m| m.version.to_string())),
                ),
                (
                    "Created by:",
                    value(meta.and_then(|m| m.created_by.clone())),
                ),
            ];
            for (label, value) in rows {
                if y >= area.y + h {
                    break;
                }
                label_value_row(
                    label,
                    &value,
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
        }
        self.render_measurements(area, buf, &mut y, LABEL_WIDTH);
    }

    /// What the open cost, under its own heading at the foot of the tab.
    ///
    /// Only what was measured: a row appears for a stretch of work that happened, and a
    /// stretch that made no requests of its own shows a time and a count and stops
    /// there. A figure datui cannot stand behind is not shown as a zero — see
    /// [`crate::measurements`] and `docs/user-guide/dataset-info.md`.
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
        // The heading and at least one row, or neither: a heading alone says a section
        // was cut off where there may have been nothing to cut. The blank line is at
        // `y`, the heading at `y + 1` and the first row at `y + 2`, so all three have
        // to fit — for every tab layout there is exactly one height at which checking
        // any fewer leaves a bare heading.
        if *y + 2 >= bottom {
            return;
        }
        *y += 1;
        Paragraph::new("Measurements").render(
            Rect {
                y: *y,
                width: area.width,
                height: 1,
                ..area
            },
            buf,
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

    /// A model file's totals, then its metadata as key and value, every value whole: a
    /// chat template wraps over as many lines as it takes, and the list scrolls.
    fn render_model_tab(&mut self, area: Rect, buf: &mut Buffer) {
        let Some(model) = self.state.model() else {
            return;
        };
        if area.height == 0 || area.width < 8 {
            return;
        }
        let g = crate::glyphs::get();
        let sep = format!(" {} ", g.middot);
        let mut head = model.kind.label();
        head.push_str(&sep);
        head.push_str(&count_of(model.tensors as u64, "tensor", "tensors"));
        if model.files > 1 {
            head.push_str(&sep);
            head.push_str(&count_of(model.files as u64, "file", "files"));
        }
        let mut lines = vec![
            head,
            format!(
                "Parameters: {}{}{sep}Size: {}",
                group_u64(model.params),
                // The short form only where it is shorter.
                if model.params >= 1000 {
                    format!(" ({})", short_count(model.params))
                } else {
                    String::new()
                },
                format_bytes(model.bytes)
            ),
        ];
        if !model.types.is_empty() {
            lines.push(format!("Types: {}", type_mix(&model.types, &sep)));
        }
        let lines: Vec<(String, Style)> = lines
            .into_iter()
            .map(|line| (line, Style::default()))
            .collect();
        self.render_detail(area, buf, &lines, &model.metadata);
    }

    /// An audio file's format, size and length, then its metadata (`bext`, iXML, `LIST
    /// INFO`, AIFF text) and markers as key and value, in a list that scrolls.
    fn render_audio_tab(&mut self, area: Rect, buf: &mut Buffer) {
        let Some(audio) = self.state.audio() else {
            return;
        };
        if area.height == 0 || area.width < 8 {
            return;
        }
        let (lines, metadata) = audio_detail(audio, self.theme);
        self.render_detail(area, buf, &lines, &metadata);
    }

    /// A detail tab's head lines, then a blank line, a `Metadata` rule and the metadata
    /// as key and value, scrolled by `detail_scroll`. Each value is drawn whole: it
    /// wraps over as many lines as it takes.
    fn render_detail(
        &mut self,
        area: Rect,
        buf: &mut Buffer,
        lines: &[(String, Style)],
        metadata: &[(String, crate::model_files::MetaValue)],
    ) {
        let width = area.width as usize;
        let mut y = area.y;
        let bottom = area.y + area.height;
        // Wrapped rather than cut: a warning cut off mid-sentence says less than nothing.
        for (line, style) in lines {
            for part in wrap_to(line, width) {
                if y >= bottom {
                    return;
                }
                Paragraph::new(clip(&part, width)).style(*style).render(
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
        // A blank line, then the rule, then at least one line of metadata, or none: a
        // file with no metadata has no rule to count it.
        if metadata.is_empty() || y + 2 >= bottom {
            return;
        }
        y += 1;
        let count = group_u64(metadata.len() as u64);
        SectionRule {
            title: "Metadata",
            chip: Some(&count),
            focused: self.modal.focus == InfoFocus::Body,
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

        let rows = metadata_lines(metadata, width);
        let room = (bottom - y) as usize;
        let fits = rows.len() <= room;
        // The last row says what is out of view when not everything fits.
        let shown = if fits { room } else { room.saturating_sub(1) };
        self.modal.detail_visible = shown;
        let max_scroll = rows.len().saturating_sub(shown);
        self.modal.detail_scroll = self.modal.detail_scroll.min(max_scroll);
        let first = self.modal.detail_scroll;
        let key_style = Style::default().fg(self.theme.text_secondary);
        for (key, value) in rows.iter().skip(first).take(shown) {
            Paragraph::new(Line::from(vec![
                Span::styled(key.clone(), key_style),
                Span::raw(value.clone()),
            ]))
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

    /// What datui noticed: each note's summary and the line saying what it is based on.
    ///
    /// Whole notes only. A note half on screen is worse than one left off: a claim with
    /// no basis under it, and a basis with no claim above it, are both the misreading
    /// the basis exists to prevent. Which notes those are is [`notes_window`]'s job,
    /// and its post-conditions are what make that true.
    ///
    /// Deliberately plain: no error styling, nothing that reads as an alarm. These are
    /// observations about the data, not faults in it.
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

        // Try the whole panel first. Only when that leaves notes out is a row needed
        // to count them, and only then do the notes have one row fewer — deciding it
        // in advance spent a row that a note which exactly fitted could have used.
        let full = area.height as usize;
        let (first, last) = notes_window(&heights, selected, self.modal.notes_scroll_offset, full);
        let all_shown = first == 0 && last == heights.len();
        // A row for the count of what is hidden, but only when something is hidden and
        // the note can spare it. A note that exactly fills the panel keeps its last
        // row: saying "no room to show one" about a note that fits is worse than not
        // saying how many are behind it.
        // A row is worth spending on the offer too: a note that says a column is not
        // read from some files, with no way to see what is there, is half a note.
        let offer = notes[selected]
            .read_as_text
            .as_ref()
            .map(|column| format!("Enter  read {column} as text"));
        let reserve = (!all_shown || offer.is_some()) && heights[selected] < full;
        let show = if reserve { full - 1 } else { full };
        if heights[selected] > show {
            // The note the cursor is on cannot show its summary and the line it rests
            // on. Drawing the summary alone would be a claim from nowhere, so say what
            // is there instead. Says "this one", not "one": a shorter note elsewhere in
            // the list may well fit, and the cursor can be moved to it.
            let count = notes.len();
            Paragraph::new(Line::from(Span::styled(
                format!(
                    "{} {}; no room for this one",
                    group_chrome(count),
                    if count == 1 { "note" } else { "notes" }
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

        // The offer and the count of what is out of view share the last row, so the
        // room goes to the count first and the offer takes what is left. The count is
        // a handful of characters and the offer is as long as a column name; giving
        // the offer its width first would push the count off the edge, and the two
        // drawn over each other read as neither.
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
            // Cut with a mark, never silently. `Enter  read measurement_value` is a
            // whole sentence that has lost `as text`, and `Enter  read me` is an offer
            // about a column called `me`; both read as something datui did not say.
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
            Paragraph::new("No partition metadata.").render(
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
            Paragraph::new("No partition columns.").render(
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

/// How long a stretch took, in a unit that does not round it away.
///
/// Milliseconds under a second, to two places. Most of these figures are under a
/// second — a local directory of a few files is walked in a fraction of a millisecond —
/// and in seconds to two places every one of them prints `0.00s`, which reads as "not
/// measured" rather than "quick".
///
/// Two places rather than one because a one-file directory's listing really is tens of
/// microseconds. There is still a floor: under five microseconds this prints
/// `0.00 ms`. Nothing datui can do makes a five-microsecond walk legible, and a figure
/// that small is honestly reported as none.
fn format_took(took: std::time::Duration) -> String {
    let ms = took.as_secs_f64() * 1000.0;
    // Rounded to the two places that are printed, then chosen. Rounding to whole
    // milliseconds instead moves the switch to 999.5 ms, which is a wide band of
    // figures the doc promises in milliseconds and would hand back in seconds; and not
    // rounding at all prints `1000.00 ms` for 999.997, which reads as larger than the
    // `1.00s` a tick later.
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
    // A byte figure only where datui counted the bytes. Everything that reports
    // requests today also weighs them; this is what stops a stretch that one day does
    // not from printing `0 B`, which would say its requests came back empty.
    if let Some(bytes) = wire.bytes {
        line.push_str(&format!(", {}", format_bytes(bytes)));
    }
    line
}

/// One measurement as a line: how long, over how many of whatever it counted, and —
/// where datui made the requests itself — how many and how much came back.
///
/// `unit` is not always "files". The listing counts the dataset's files; a footer pass
/// counts footers read, and those are not the same number — a dataset that opens before
/// its footers are read has them read again behind the open, and one that cannot settle
/// its row count reads them all again to count. Calling both "files" would put a figure
/// larger than the dataset under the word the listing uses for the dataset's size.
fn measurement_line(cost: &crate::measurements::Cost, unit: &str, singular: &str) -> String {
    let mut line = format_took(cost.took);
    // A stretch that never learned a count says a time and stops, rather than putting
    // a number that is not the size of the dataset under the word the other rows use
    // for exactly that.
    if let Some(files) = cost.files {
        let unit = if files == 1 { singular } else { unit };
        line.push_str(&format!(", {} {unit}", format_int(files)));
    }
    if let Some(wire) = cost.over_the_wire {
        line.push_str(&wire_line(wire));
    }
    line
}

/// The total as a line: a time, and the requests behind it.
///
/// No file count, deliberately — see [`crate::measurements::Meter::total`].
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
        let offered = TabsOffered::of(self.state);
        let tab = self.modal.active_tab;
        let on_tab_bar = self.modal.focus == InfoFocus::TabBar;

        // The panel's own keys, said where they work and only while they work:
        // nothing here may live only in `?`.
        let g = crate::glyphs::get();
        let scrolls = match tab {
            InfoTab::Schema => !on_tab_bar,
            InfoTab::Notes => offered.notes,
            InfoTab::Model => offered.model,
            InfoTab::Audio => offered.audio,
            _ => false,
        };
        let mut footer = HintBar::from_ctx(ctx).hint_weighted(g.updown_lr, "Tabs", 3);
        if scrolls {
            footer = footer.hint_weighted(g.updown, "Scroll", 2);
        }
        if tab == InfoTab::Schema {
            footer = footer.hint_weighted("Tab", "Focus", 1);
        }
        let footer = footer.hint_weighted("Esc", "Close", 4);
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

        // Tab line: the active tab carries the accent, and the rail sits beside
        // its name while the tab bar holds focus. The slot is reserved either
        // way, so focus arriving or leaving moves nothing.
        let tabs = InfoTab::visible(offered);
        let active = tabs[tab.index(offered)];
        let mut spans = Vec::new();
        for (i, t) in tabs.iter().enumerate() {
            let is_active = *t == active;
            if i > 0 {
                spans.push(Span::styled(
                    format!(" {}", g.rule),
                    Style::default().fg(ctx.dimmed),
                ));
            }
            let mark = if on_tab_bar && is_active { g.rail } else { " " };
            spans.push(Span::styled(mark, Style::default().fg(ctx.accent)));
            let style = if is_active {
                Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(ctx.text_secondary)
            };
            spans.push(Span::styled(t.title(), style));
        }
        Paragraph::new(Line::from(spans)).render(
            Rect {
                height: tab_rows,
                ..content
            },
            buf,
        );

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
            InfoTab::Model if offered.model => self.render_model_tab(body, buf),
            InfoTab::Audio if offered.audio => self.render_audio_tab(body, buf),
            InfoTab::Partitions if offered.partitions => {
                self.render_partitioned_data_tab(body, buf)
            }
            InfoTab::Notes if offered.notes => self.render_notes_tab(body, buf),
            InfoTab::Model | InfoTab::Audio | InfoTab::Partitions | InfoTab::Notes => {
                self.render_schema_tab(body, buf)
            }
        }
    }
}

/// Read Parquet metadata from path. Returns `None` on error. Blocking.
pub fn read_parquet_metadata(path: &Path) -> Option<ParquetMetadataCache> {
    let mut f = std::fs::File::open(path).ok()?;
    let meta = read_metadata(&mut f).ok()?;
    Some(Arc::new(meta))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The optional tabs, as the tests before the Model tab named them.
    fn offer(partitions: bool, notes: bool) -> TabsOffered {
        TabsOffered {
            model: false,
            audio: false,
            partitions,
            notes,
        }
    }

    /// What a file says about itself: a size and, for Parquet, a footer; a directory
    /// has no size of its own to give; a file that is gone, or whose footer is not
    /// one, is a reason rather than a blank.
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

        assert!(matches!(
            FileFacts::read(&csv, false),
            Ok(FileFacts::Read {
                size: Some(4),
                parquet: None
            })
        ));
        match FileFacts::read(&parquet, true) {
            Ok(FileFacts::Read {
                size: Some(size),
                parquet: Some(footer),
            }) => {
                assert_eq!(size, parquet_len);
                assert_eq!(footer.num_rows, 3);
            }
            other => panic!("a Parquet file's size and footer: {other:?}"),
        }
        assert!(matches!(
            FileFacts::read(dir.path(), true),
            Ok(FileFacts::Read {
                size: None,
                parquet: None
            })
        ));
        // Reasons short enough for the panel's one line.
        assert_eq!(
            FileFacts::read(&dir.path().join("gone.parquet"), true).unwrap_err(),
            "file not found"
        );
        assert_eq!(
            FileFacts::read(&csv, true).expect_err("a CSV has no footer"),
            "unreadable Parquet footer"
        );
    }

    /// A dataset that has not been counted says so rather than showing how far it got.
    ///
    /// Through a rendered panel, not the helper: the helper cannot tell whether its
    /// caller passed `num_rows_if_valid()` or the raw field, and the raw field is what
    /// the bug was.
    #[test]
    fn the_schema_tab_does_not_call_a_partial_the_total() {
        use crate::widgets::datatable::DataTableState;
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
                    parquet_file: false,
                },
                &mut modal,
                &theme,
            );
            panel.render_schema_summary(area, &mut buf);
            (0..area.height)
                .map(|y| {
                    (0..area.width)
                        .map(|x| buf[(x, y)].symbol().to_string())
                        .collect::<String>()
                })
                .collect::<Vec<_>>()
                .join("\n")
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
        use crate::widgets::datatable::DataTableState;
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
                parquet_file: false,
            },
            &mut modal,
            &theme,
        );
        (&mut panel).render(area, &mut buf);
        let text = (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.contains("below"),
            "the hidden columns are counted: {text}"
        );
        assert!(text.contains("Esc"), "the footer names the way out: {text}");
        assert!(text.contains("Tabs"), "and the tab keys: {text}");
        assert!(!text.contains(">>"), "the bespoke marker is gone: {text}");
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

    /// The Resources tab shows what the open cost, and shows only what was measured.
    ///
    /// Through the rendered tab rather than [`measurement_line`], because the bug worth
    /// guarding is a row reaching the panel for a stretch of work that never ran — a
    /// dataset opened before any of this existed would otherwise read as one whose
    /// listing took no time at all.
    #[test]
    fn the_resources_tab_shows_what_was_measured_and_nothing_else() {
        use crate::measurements::Meter;
        use crate::widgets::datatable::DataTableState;
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
            .with_open(crate::widgets::datatable::OpenFacts {
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
                    parquet_file: false,
                },
                &mut modal,
                &theme,
            );
            panel.render_resources_tab(area, &mut buf);
            (0..area.height)
                .map(|y| {
                    (0..area.width)
                        .map(|x| buf[(x, y)].symbol().to_string())
                        .collect::<String>()
                })
                .collect::<Vec<_>>()
                .join("\n")
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
        use crate::widgets::datatable::{DataTableState, OpenFacts};
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
                    parquet_file: false,
                },
                &mut modal,
                &theme,
            );
            (&mut panel).render(area, &mut buf);
            (0..area.height)
                .map(|y| {
                    (0..area.width)
                        .map(|x| buf[(x, y)].symbol().to_string())
                        .collect::<String>()
                })
                .collect::<Vec<_>>()
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

    /// Focus is the accent: in the schema, the section rule brightens and the row
    /// carries the rail; on the tab bar, the rail sits beside the active tab and
    /// the row keeps only the accent. One frame either way, the footer inside it.
    #[test]
    fn focus_moves_the_accent_between_the_tab_bar_and_the_schema() {
        use crate::widgets::datatable::DataTableState;
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

        let paint = |focus: InfoFocus| {
            let area = Rect::new(0, 0, 50, 16);
            let mut buf = Buffer::empty(area);
            let mut modal = InfoModal::default();
            modal.open();
            modal.focus = focus;
            let mut panel = DataTableInfo::new(
                &state,
                InfoContext {
                    format: None,
                    facts: None,
                    parquet_file: false,
                },
                &mut modal,
                &theme,
            );
            (&mut panel).render(area, &mut buf);
            let text: Vec<String> = (0..area.height)
                .map(|y| {
                    (0..area.width)
                        .map(|x| buf[(x, y)].symbol().to_string())
                        .collect()
                })
                .collect();
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

        // The body has focus: the rule is bright, the id row carries the rail,
        // and the tab line has none.
        let (buf, text) = paint(InfoFocus::Body);
        let (x, y) = find(&text, "Schema: Inferred");
        assert_eq!(buf[(x, y)].fg, theme.accent_bright, "{text:#?}");
        let (_, id_row) = find(&text, " id ");
        assert!(text[id_row as usize].contains(g.rail), "{text:#?}");
        let (x, y) = find(&text, "Resources");
        assert!(!text[y as usize].contains(g.rail), "{text:#?}");
        assert_ne!(
            buf[(x, y)].fg,
            theme.accent,
            "an inactive tab is not accented"
        );

        // The tab bar has focus: the rail moves beside the active tab, and the
        // row the cursor is on keeps the accent without the rail.
        let (buf, text) = paint(InfoFocus::TabBar);
        let (_, tab_row) = find(&text, "Resources");
        assert!(
            text[tab_row as usize].contains(&format!("{}Schema", g.rail)),
            "{text:#?}"
        );
        let (x, y) = find(&text, "Schema: Inferred");
        assert_eq!(buf[(x, y)].fg, theme.accent, "{text:#?}");
        let (id_x, id_row) = find(&text, " id ");
        assert!(!text[id_row as usize].contains(g.rail), "{text:#?}");
        assert_eq!(buf[(id_x + 1, id_row)].fg, theme.accent, "{text:#?}");

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
    fn the_model_tab_sits_beside_the_schema() {
        let offered = TabsOffered {
            model: true,
            audio: false,
            partitions: false,
            notes: true,
        };
        assert_eq!(
            InfoTab::visible(offered),
            [
                InfoTab::Schema,
                InfoTab::Model,
                InfoTab::Resources,
                InfoTab::Notes
            ]
        );
        assert_eq!(InfoTab::Schema.next(offered), InfoTab::Model);
        assert_eq!(InfoTab::Model.index(offer(false, false)), 0, "not offered");
    }

    #[test]
    fn the_audio_tab_sits_beside_the_schema() {
        let offered = TabsOffered {
            audio: true,
            ..offer(false, false)
        };
        assert_eq!(
            InfoTab::visible(offered),
            [InfoTab::Schema, InfoTab::Audio, InfoTab::Resources]
        );
        assert_eq!(InfoTab::Audio.prev(offered), InfoTab::Schema);
    }

    #[test]
    fn a_clock_shows_hours_only_when_there_are_some() {
        assert_eq!(clock(3.25), "0:03.250");
        assert_eq!(clock(62.0), "1:02.000");
        assert_eq!(clock(3723.0005), "1:02:03.001");
    }

    /// Each value is drawn whole: its own newlines kept, wrapped under the key, a short
    /// array listed and a long one counted.
    #[test]
    fn metadata_values_wrap_whole_under_their_key() {
        use crate::model_files::MetaValue;
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
                (blank.clone(), "second line ".to_string()),
                (blank.clone(), "that is long".to_string()),
                (key("tokens"), "[151,936 str".to_string()),
                (blank.clone(), "ings]".to_string()),
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
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1536), "1.5 KiB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MiB");
    }

    #[test]
    fn test_format_int() {
        assert_eq!(format_int(0), "0");
        assert_eq!(format_int(1234), "1,234");
        assert_eq!(format_int(1_234_567), "1,234,567");
    }
}
