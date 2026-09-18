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
use ratatui::widgets::{
    Block, BorderType, Borders, Gauge, Padding, Paragraph, Row, StatefulWidget, Table, Tabs, Widget,
};

use super::datatable::DataTableState;
use crate::export_modal::ExportFormat;

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
    let marker = if selected { "› " } else { "  " };
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
fn wrap_to(text: &str, width: usize) -> Vec<String> {
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
    Resources,
    Partitions,
    Notes,
}

impl InfoTab {
    /// The tabs on offer, in order: Partitions only for a partitioned dataset, Notes
    /// only when datui has something to say about the data.
    pub fn visible(has_partitions: bool, has_notes: bool) -> Vec<InfoTab> {
        let mut tabs = vec![InfoTab::Schema, InfoTab::Resources];
        if has_partitions {
            tabs.push(InfoTab::Partitions);
        }
        if has_notes {
            tabs.push(InfoTab::Notes);
        }
        tabs
    }

    pub fn title(self) -> &'static str {
        match self {
            InfoTab::Schema => "Schema",
            InfoTab::Resources => "Resources",
            InfoTab::Partitions => "Partitions",
            InfoTab::Notes => "Notes",
        }
    }

    /// Next tab, wrapping. A tab that is not on offer starts from the first.
    pub fn next(self, has_partitions: bool, has_notes: bool) -> Self {
        let tabs = Self::visible(has_partitions, has_notes);
        let at = self.index(has_partitions, has_notes);
        tabs[(at + 1) % tabs.len()]
    }

    pub fn prev(self, has_partitions: bool, has_notes: bool) -> Self {
        let tabs = Self::visible(has_partitions, has_notes);
        let at = self.index(has_partitions, has_notes);
        tabs[(at + tabs.len() - 1) % tabs.len()]
    }

    /// Where this tab sits among the ones on offer; 0 when it is not among them.
    pub fn index(self, has_partitions: bool, has_notes: bool) -> usize {
        Self::visible(has_partitions, has_notes)
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
}

impl InfoModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(&mut self) {
        self.active = true;
        self.active_tab = InfoTab::Schema;
        self.focus = InfoFocus::Body;
        self.schema_selected_index = 0;
        self.schema_scroll_offset = 0;
        self.schema_table_state.select(Some(0));
        self.notes_selected_index = 0;
        self.notes_scroll_offset = 0;
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

    /// Switch to next tab; `has_partitions` determines whether Partitions tab is available.
    pub fn switch_tab(&mut self, has_partitions: bool, has_notes: bool) {
        self.active_tab = self.active_tab.next(has_partitions, has_notes);
        if self.active_tab == InfoTab::Schema {
            self.schema_selected_index = 0;
            self.schema_scroll_offset = 0;
            self.schema_table_state.select(Some(0));
        } else {
            self.focus = InfoFocus::TabBar;
        }
    }

    /// Switch to previous tab; `has_partitions` determines whether Partitions tab is available.
    pub fn switch_tab_prev(&mut self, has_partitions: bool, has_notes: bool) {
        self.active_tab = self.active_tab.prev(has_partitions, has_notes);
        if self.active_tab == InfoTab::Schema {
            self.schema_selected_index = 0;
            self.schema_scroll_offset = 0;
            self.schema_table_state.select(Some(0));
        } else {
            self.focus = InfoFocus::TabBar;
        }
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

/// Context for the info panel: path, format, optional Parquet metadata.
pub struct InfoContext<'a> {
    pub path: Option<&'a Path>,
    pub format: Option<ExportFormat>,
    pub parquet_metadata: Option<&'a ParquetMetadataCache>,
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

    pub fn file_size_bytes(&self) -> Option<u64> {
        self.path
            .and_then(|p| std::fs::metadata(p).ok())
            .map(|m| m.len())
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
            .unwrap_or_else(|| "—".to_string());
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
    pub border_color: ratatui::style::Color,
    pub active_color: ratatui::style::Color,
    pub primary_color: ratatui::style::Color,
    /// Style of the schema row the cursor is on.
    pub highlight: Style,
}

impl<'a> DataTableInfo<'a> {
    pub fn new(
        state: &'a DataTableState,
        ctx: InfoContext<'a>,
        modal: &'a mut InfoModal,
        border_color: ratatui::style::Color,
        active_color: ratatui::style::Color,
        primary_color: ratatui::style::Color,
        highlight: Style,
    ) -> Self {
        Self {
            state,
            ctx,
            modal,
            border_color,
            active_color,
            primary_color,
            highlight,
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
        let ncols = self.state.schema.len();
        let nrows = self.state.num_rows;
        let mut lines = vec![];
        lines.push(format!(
            "Rows (total): {} · Columns: {}",
            format_int(nrows),
            ncols
        ));
        let by_type = columns_by_type(self.state.schema.as_ref());
        if !by_type.is_empty() {
            lines.push(by_type);
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
        let src = match self.state.dataset_schema() {
            Some(dataset) => dataset.origin.to_string(),
            None => self.ctx.schema_source().to_string(),
        };
        let compression = self
            .ctx
            .parquet_metadata
            .map(|m| parquet_column_compression(m.as_ref(), self.state.schema.as_ref()));
        let has_comp = compression.as_ref().is_some_and(|c| !c.is_empty());
        let header = if has_comp {
            Row::new(vec!["Column", "Type", "Source", "Compression"]).bold()
        } else {
            Row::new(vec!["Column", "Type", "Source"]).bold()
        };

        let total_rows = self.state.schema.len();
        let body_focused = self.modal.focus == InfoFocus::Body;
        let border_style = if body_focused {
            Style::default().fg(self.active_color)
        } else {
            Style::default().fg(self.border_color)
        };
        let block = Block::default()
            .title(Line::from(format!("Schema: {}", src)).bold())
            .title_style(ratatui::style::Style::reset())
            .padding(Padding::new(1, 1, 1, 1))
            .border_style(border_style);
        let inner = block.inner(area);
        let visible_height = inner.height as usize;
        block.render(area, buf);

        let data_height = visible_height.saturating_sub(1);
        self.modal.schema_visible_height = data_height;
        self.modal.sync_schema_table_state(total_rows, data_height);

        let offset = self.modal.schema_scroll_offset;
        let take = visible_height
            .saturating_sub(1)
            .min(total_rows.saturating_sub(offset));
        let mut rows = vec![];
        for (idx, (name, dtype)) in self.state.schema.iter().enumerate() {
            if idx < offset {
                continue;
            }
            if idx >= offset + take {
                break;
            }
            let name_str: &str = name.as_ref();
            let comp_str = compression
                .as_ref()
                .and_then(|c| c.get(name_str))
                .map(|(codec, ratio)| format!("{} {:.1}×", codec, ratio))
                .unwrap_or_else(|| "—".to_string());
            let row = if has_comp {
                Row::new(vec![
                    name.to_string(),
                    dtype.to_string(),
                    src.to_string(),
                    comp_str,
                ])
            } else {
                Row::new(vec![name.to_string(), dtype.to_string(), src.to_string()])
            };
            rows.push(row);
        }

        let widths: Vec<Constraint> = if has_comp {
            vec![
                Constraint::Percentage(25),
                Constraint::Percentage(35),
                Constraint::Percentage(15),
                Constraint::Percentage(25),
            ]
        } else {
            vec![
                Constraint::Percentage(40),
                Constraint::Percentage(40),
                Constraint::Percentage(20),
            ]
        };
        let table = Table::new(rows, widths)
            .header(header)
            .column_spacing(1)
            .row_highlight_style(self.highlight)
            .highlight_symbol(">> ");
        StatefulWidget::render(table, inner, buf, &mut self.modal.schema_table_state);
    }

    fn render_resources_tab(&self, area: Rect, buf: &mut Buffer) {
        const LABEL_WIDTH: u16 = 16;
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
        let file_size = self.ctx.file_size_bytes().map(format_bytes);
        let file_size_str = file_size.as_deref().unwrap_or("—");
        label_value_row(
            "File size:",
            file_size_str,
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
        let fmt = self.ctx.format.map(|f| f.as_str()).unwrap_or("—");
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
                .gauge_style(Style::default().fg(self.primary_color))
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
                None => "—".to_string(),
            };
            Gauge::default()
                .gauge_style(Style::default().fg(self.primary_color))
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
                        .unwrap_or_else(|| "—".to_string())
                });
            Paragraph::new(value).render(mb_chunks[1], buf);
        }
        y += 1;

        if let Some(ref meta) = self.ctx.parquet_metadata {
            let (comp, uncomp) = parquet_overall_sizes(meta.as_ref());
            if comp > 0 && uncomp > 0 && y < area.y + h {
                let ratio = uncomp as f64 / comp as f64;
                let value = format!("{:.1}× (uncomp. {})", ratio, format_bytes(uncomp));
                label_value_row(
                    "Parquet comp.:",
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
            if y < area.y + h {
                label_value_row(
                    "Row groups:",
                    &meta.row_groups.len().to_string(),
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
            if y < area.y + h {
                label_value_row(
                    "Parquet version:",
                    &meta.version.to_string(),
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
            if let Some(ref cb) = meta.created_by
                && y < area.y + h
            {
                label_value_row(
                    "Created by:",
                    cb,
                    Rect {
                        y,
                        width: w,
                        height: 1,
                        ..area
                    },
                    buf,
                    LABEL_WIDTH,
                );
            }
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
        let dim = Style::default().fg(self.border_color);

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

        if let (true, Some(offer)) = (reserve, offer.as_ref()) {
            Paragraph::new(Line::from(Span::styled(offer.as_str(), dim))).render(
                Rect {
                    y: area.y + area.height - 1,
                    height: 1,
                    ..area
                },
                buf,
            );
        }
        let (above, below) = (first, notes.len() - last);
        if reserve && (above > 0 || below > 0) {
            let hidden = match (above, below) {
                (0, n) => format!("{} below", group_chrome(n)),
                (n, 0) => format!("{} above", group_chrome(n)),
                (a, b) => format!("{} above, {} below", group_chrome(a), group_chrome(b)),
            };
            Paragraph::new(Line::from(Span::styled(hidden, dim)))
                .right_aligned()
                .render(
                    Rect {
                        y: area.y + area.height - 1,
                        height: 1,
                        ..area
                    },
                    buf,
                );
        }
    }

    fn render_partitioned_data_tab(&self, area: Rect, buf: &mut Buffer) {
        let y = area.y;
        let w = area.width;

        let Some(partition_columns) = self.state.partition_columns.as_ref() else {
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
        .join(" · ")
}

impl<'a> Widget for &mut DataTableInfo<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let tab_bar_focused = self.modal.focus == InfoFocus::TabBar;
        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .title("Info")
            .title_style(ratatui::style::Style::reset());

        let inner = block.inner(area);
        block.render(area, buf);

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(2), Constraint::Min(4)])
            .split(inner);

        let tab_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1)])
            .split(chunks[0]);

        let has_partitions = self
            .state
            .partition_columns
            .as_ref()
            .map(|v| !v.is_empty())
            .unwrap_or(false);
        let has_notes = self.state.has_notes();
        let tab_titles: Vec<&str> = InfoTab::visible(has_partitions, has_notes)
            .into_iter()
            .map(InfoTab::title)
            .collect();
        let sel = self.modal.active_tab.index(has_partitions, has_notes);
        let tabs = Tabs::new(tab_titles)
            .style(Style::default().fg(self.border_color))
            .highlight_style(
                Style::default()
                    .fg(self.active_color)
                    .add_modifier(Modifier::REVERSED),
            )
            .select(sel);
        tabs.render(tab_chunks[0], buf);
        let line_style = if tab_bar_focused {
            Style::default().fg(self.active_color)
        } else {
            Style::default().fg(self.border_color)
        };
        Block::default()
            .borders(Borders::BOTTOM)
            .border_type(BorderType::Rounded)
            .border_style(line_style)
            .render(tab_chunks[1], buf);

        match self.modal.active_tab {
            InfoTab::Schema => self.render_schema_tab(chunks[1], buf),
            InfoTab::Resources => self.render_resources_tab(chunks[1], buf),
            InfoTab::Partitions => {
                if has_partitions {
                    self.render_partitioned_data_tab(chunks[1], buf)
                } else {
                    self.render_schema_tab(chunks[1], buf)
                }
            }
            InfoTab::Notes if has_notes => self.render_notes_tab(chunks[1], buf),
            InfoTab::Notes => self.render_schema_tab(chunks[1], buf),
        }
    }
}

/// Read Parquet metadata from path. Returns `None` on error.
pub fn read_parquet_metadata(path: &Path) -> Option<ParquetMetadataCache> {
    let mut f = std::fs::File::open(path).ok()?;
    let meta = read_metadata(&mut f).ok()?;
    Some(Arc::new(meta))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_tabs_on_offer_depend_on_the_dataset() {
        assert_eq!(
            InfoTab::visible(false, false),
            [InfoTab::Schema, InfoTab::Resources]
        );
        assert_eq!(
            InfoTab::visible(true, true),
            [
                InfoTab::Schema,
                InfoTab::Resources,
                InfoTab::Partitions,
                InfoTab::Notes
            ]
        );
        assert_eq!(
            InfoTab::visible(false, true),
            [InfoTab::Schema, InfoTab::Resources, InfoTab::Notes],
            "notes without partitions still sit last"
        );
    }

    #[test]
    fn tab_navigation_wraps_through_what_is_on_offer() {
        // Nothing optional: two tabs, back and forth.
        assert_eq!(InfoTab::Schema.next(false, false), InfoTab::Resources);
        assert_eq!(InfoTab::Resources.next(false, false), InfoTab::Schema);
        assert_eq!(InfoTab::Schema.prev(false, false), InfoTab::Resources);

        // Both optional tabs present.
        assert_eq!(InfoTab::Resources.next(true, true), InfoTab::Partitions);
        assert_eq!(InfoTab::Partitions.next(true, true), InfoTab::Notes);
        assert_eq!(InfoTab::Notes.next(true, true), InfoTab::Schema);
        assert_eq!(InfoTab::Schema.prev(true, true), InfoTab::Notes);

        // Notes only.
        assert_eq!(InfoTab::Resources.next(false, true), InfoTab::Notes);
        assert_eq!(InfoTab::Notes.prev(false, true), InfoTab::Resources);
    }

    /// A tab that is no longer on offer must not strand the cursor: it reads as the
    /// first tab, so moving on from it goes somewhere real.
    #[test]
    fn a_tab_that_is_no_longer_offered_falls_back_to_the_first() {
        assert_eq!(InfoTab::Notes.index(false, false), 0);
        assert_eq!(InfoTab::Notes.next(false, false), InfoTab::Resources);
        assert_eq!(InfoTab::Partitions.index(false, false), 0);
        assert_eq!(InfoTab::Partitions.prev(false, false), InfoTab::Resources);
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
