//! Reusable radio-button block: a bordered block with a grid of options (● selected, ○ unselected).
//! Used for chart type, chart export format, and pivot aggregation.

use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Paragraph, Widget},
};

/// Renders a block of radio options. Options are laid out in a grid with `columns` per row.
/// Selected item is drawn with ● and highlighted when focused; others with ○.
pub struct RadioBlock<'a> {
    pub title: &'a str,
    pub options: &'a [&'a str],
    pub selected: usize,
    pub focused: bool,
    pub columns: usize,
    pub border_color: ratatui::style::Color,
    pub active_color: ratatui::style::Color,
}

impl<'a> RadioBlock<'a> {
    pub fn new(
        title: &'a str,
        options: &'a [&'a str],
        selected: usize,
        focused: bool,
        columns: usize,
        border_color: ratatui::style::Color,
        active_color: ratatui::style::Color,
    ) -> Self {
        Self {
            title,
            options,
            selected,
            focused,
            columns: columns.max(1),
            border_color,
            active_color,
        }
    }

    fn render_inner(&self, area: Rect, buf: &mut Buffer) {
        if self.options.is_empty() {
            return;
        }
        let n = self.options.len();
        let cols = self.columns.min(n);
        let rows = n.div_ceil(cols);

        let row_constraints: Vec<Constraint> = (0..rows).map(|_| Constraint::Length(1)).collect();
        let row_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(row_constraints)
            .split(area);

        let col_width = area.width / cols as u16;
        let col_constraints: Vec<Constraint> =
            (0..cols).map(|_| Constraint::Length(col_width)).collect();

        for (idx, label) in self.options.iter().enumerate() {
            let row = idx / cols;
            let col = idx % cols;
            if row >= row_chunks.len() {
                break;
            }
            let row_rect = row_chunks[row];
            let col_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints(col_constraints.as_slice())
                .split(row_rect);
            let cell = col_chunks[col];

            let is_selected = idx == self.selected;
            let marker = if is_selected { "●" } else { "○" };
            let text = format!("{} {}", marker, *label);
            let style = if is_selected {
                Style::default().fg(self.active_color)
            } else {
                Style::default().fg(self.border_color)
            };
            let style = if self.focused && is_selected {
                style.add_modifier(Modifier::REVERSED)
            } else {
                style
            };
            Paragraph::new(Line::from(Span::styled(text, style))).render(cell, buf);
        }
    }
}

impl Widget for RadioBlock<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block_style = if self.focused {
            Style::default().fg(self.active_color)
        } else {
            Style::default().fg(self.border_color)
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .title(self.title)
            .border_style(block_style);
        let inner = block.inner(area);
        block.render(area, buf);
        self.render_inner(inner, buf);
    }
}
