//! Info panel: tabbed Schema and Resources view for dataset technical info.

use std::collections::HashMap;
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
use ratatui::text::Line;
use ratatui::widgets::{
    Block, Borders, Padding, Paragraph, Row, StatefulWidget, Table, Tabs, Widget,
};

use super::datatable::DataTableState;
use crate::export_modal::ExportFormat;

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
}

impl InfoTab {
    pub fn next(self) -> Self {
        match self {
            InfoTab::Schema => InfoTab::Resources,
            InfoTab::Resources => InfoTab::Schema,
        }
    }
    pub fn prev(self) -> Self {
        self.next()
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

    pub fn switch_tab(&mut self) {
        self.active_tab = self.active_tab.next();
        if self.active_tab == InfoTab::Schema {
            self.schema_selected_index = 0;
            self.schema_scroll_offset = 0;
            self.schema_table_state.select(Some(0));
        } else {
            self.focus = InfoFocus::TabBar;
        }
    }

    /// Scroll and selection for schema table. `total_rows` = schema len, `visible_height` = rows shown.
    /// Returns true if state changed.
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
            Some(ExportFormat::Parquet) => "Known",
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
}

impl<'a> DataTableInfo<'a> {
    pub fn new(
        state: &'a DataTableState,
        ctx: InfoContext<'a>,
        modal: &'a mut InfoModal,
        border_color: ratatui::style::Color,
        active_color: ratatui::style::Color,
    ) -> Self {
        Self {
            state,
            ctx,
            modal,
            border_color,
            active_color,
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
        let src = self.ctx.schema_source();
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
            .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
            .highlight_symbol(">> ");
        StatefulWidget::render(table, inner, buf, &mut self.modal.schema_table_state);
    }

    fn render_resources_tab(&self, area: Rect, buf: &mut Buffer) {
        let mut y = area.y;
        let w = area.width;

        let file_size = self.ctx.file_size_bytes().map(format_bytes);
        let line = format!("File size: {}", file_size.as_deref().unwrap_or("—"));
        Paragraph::new(line).render(
            Rect {
                y,
                width: w,
                height: 1,
                ..area
            },
            buf,
        );
        y += 1;

        let mem = self
            .state
            .buffered_memory_bytes()
            .map(|b| format_bytes(b as u64));
        let line = format!("Buffered (visible): {}", mem.as_deref().unwrap_or("—"));
        Paragraph::new(line).render(
            Rect {
                y,
                width: w,
                height: 1,
                ..area
            },
            buf,
        );
        y += 1;

        if let Some(ref meta) = self.ctx.parquet_metadata {
            let (comp, uncomp) = parquet_overall_sizes(meta.as_ref());
            if comp > 0 && uncomp > 0 {
                let ratio = uncomp as f64 / comp as f64;
                let line = format!(
                    "Parquet compression: {:.1}× (uncomp. {})",
                    ratio,
                    format_bytes(uncomp)
                );
                Paragraph::new(line).render(
                    Rect {
                        y,
                        width: w,
                        height: 1,
                        ..area
                    },
                    buf,
                );
                y += 1;
            }
            let line = format!("Row groups: {}", meta.row_groups.len());
            Paragraph::new(line).render(
                Rect {
                    y,
                    width: w,
                    height: 1,
                    ..area
                },
                buf,
            );
            y += 1;
            let line = format!("Parquet version: {}", meta.version);
            Paragraph::new(line).render(
                Rect {
                    y,
                    width: w,
                    height: 1,
                    ..area
                },
                buf,
            );
            y += 1;
            if let Some(ref cb) = meta.created_by {
                let line = format!("Created by: {}", cb);
                Paragraph::new(line).render(
                    Rect {
                        y,
                        width: w,
                        height: 1,
                        ..area
                    },
                    buf,
                );
                y += 1;
            }
        }

        let fmt = self.ctx.format.map(|f| f.as_str()).unwrap_or("—");
        let line = format!("Format: {}", fmt);
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

fn format_int(n: usize) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.insert(0, ',');
        }
        out.insert(0, c);
    }
    out
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
        let block = Block::default().borders(Borders::ALL).title("Info");

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

        let sel = match self.modal.active_tab {
            InfoTab::Schema => 0,
            InfoTab::Resources => 1,
        };
        let tabs = Tabs::new(vec!["Schema", "Resources"])
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
            .border_style(line_style)
            .render(tab_chunks[1], buf);

        match self.modal.active_tab {
            InfoTab::Schema => self.render_schema_tab(chunks[1], buf),
            InfoTab::Resources => self.render_resources_tab(chunks[1], buf),
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
