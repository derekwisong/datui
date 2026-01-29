//! Chart export modal rendering: format (PNG/EPS) and path.

use crate::chart_export::ChartExportFormat;
use crate::chart_export_modal::{ChartExportFocus, ChartExportModal};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Widget};

const FORMAT_COLS: u16 = 3;

pub fn render_chart_export_modal(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ChartExportModal,
    border_color: ratatui::style::Color,
    active_color: ratatui::style::Color,
) {
    Clear.render(area, buf);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
        .title(" Export Chart ");
    let inner = block.inner(area);
    block.render(area, buf);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Format row
            Constraint::Length(3), // Path row
            Constraint::Length(3), // Buttons: just tall enough for label + border
        ])
        .split(inner);

    // Format: 3-column grid, only as many rows as needed (2 options = 1 row)
    let format_area = chunks[0];
    let is_format_focused = modal.focus == ChartExportFocus::FormatSelector;
    let format_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if is_format_focused {
            active_color
        } else {
            border_color
        }))
        .title(" Format ");
    let format_inner = format_block.inner(format_area);
    format_block.render(format_area, buf);

    let col_width = format_inner.width / FORMAT_COLS;
    for (i, &format) in ChartExportFormat::ALL.iter().enumerate() {
        let row = i / FORMAT_COLS as usize;
        let col = i % FORMAT_COLS as usize;
        let cell_x = format_inner.x + (col as u16 * col_width);
        let cell_y = format_inner.y + row as u16;
        if cell_y >= format_inner.bottom() {
            break;
        }
        let cell_area = Rect {
            x: cell_x,
            y: cell_y,
            width: col_width,
            height: 1,
        };
        let marker = if modal.selected_format == format {
            "●"
        } else {
            "○"
        };
        let style = if modal.selected_format == format {
            Style::default().fg(active_color)
        } else {
            Style::default().fg(border_color)
        };
        Paragraph::new(Line::from(Span::styled(
            format!("{} {}", marker, format.as_str()),
            style,
        )))
        .render(cell_area, buf);
    }

    // Path input
    let path_area = chunks[1];
    let is_path_focused = modal.focus == ChartExportFocus::PathInput;
    let path_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if is_path_focused {
            active_color
        } else {
            border_color
        }))
        .title(" File Path ");
    let path_inner = path_block.inner(path_area);
    path_block.render(path_area, buf);
    modal.path_input.set_focused(is_path_focused);
    (&modal.path_input).render(path_inner, buf);

    // Buttons
    let btn_area = chunks[2];
    let btn_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(btn_area);

    let is_export_focused = modal.focus == ChartExportFocus::ExportButton;
    Paragraph::new("Export")
        .style(if is_export_focused {
            Style::default().fg(active_color)
        } else {
            Style::default().fg(border_color)
        })
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(if is_export_focused {
                    Style::default().fg(active_color)
                } else {
                    Style::default().fg(border_color)
                }),
        )
        .centered()
        .render(btn_chunks[0], buf);

    let is_cancel_focused = modal.focus == ChartExportFocus::CancelButton;
    Paragraph::new("Cancel")
        .style(if is_cancel_focused {
            Style::default().fg(active_color)
        } else {
            Style::default().fg(border_color)
        })
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(if is_cancel_focused {
                    Style::default().fg(active_color)
                } else {
                    Style::default().fg(border_color)
                }),
        )
        .centered()
        .render(btn_chunks[1], buf);
}
