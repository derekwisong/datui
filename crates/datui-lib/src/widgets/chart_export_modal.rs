//! Chart export modal rendering: format (PNG/EPS), optional title, and path.

use crate::chart_export::ChartExportFormat;
use crate::chart_export_modal::{ChartExportFocus, ChartExportModal};
use crate::widgets::radio_block::RadioBlock;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Style;
use ratatui::widgets::{Block, BorderType, Borders, Clear, Paragraph, Widget};

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
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(border_color))
        .title(" Export Chart ");
    let inner = block.inner(area);
    block.render(area, buf);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Format row
            Constraint::Length(3), // Chart title (optional)
            Constraint::Length(3), // Path row
            Constraint::Length(3), // Buttons
        ])
        .split(inner);

    // Format: radio block (PNG / EPS)
    let format_area = chunks[0];
    let is_format_focused = modal.focus == ChartExportFocus::FormatSelector;
    let format_labels: Vec<&str> = ChartExportFormat::ALL.iter().map(|f| f.as_str()).collect();
    let format_selected = ChartExportFormat::ALL
        .iter()
        .position(|&f| f == modal.selected_format)
        .unwrap_or(0);
    RadioBlock::new(
        " Format ",
        &format_labels,
        format_selected,
        is_format_focused,
        2,
        border_color,
        active_color,
    )
    .render(format_area, buf);

    // Chart title (optional; blank = no title on export)
    let title_area = chunks[1];
    let is_title_focused = modal.focus == ChartExportFocus::TitleInput;
    let title_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(if is_title_focused {
            active_color
        } else {
            border_color
        }))
        .title(" Chart Title ");
    let title_inner = title_block.inner(title_area);
    title_block.render(title_area, buf);
    modal.title_input.set_focused(is_title_focused);
    (&modal.title_input).render(title_inner, buf);

    // Path input
    let path_area = chunks[2];
    let is_path_focused = modal.focus == ChartExportFocus::PathInput;
    let path_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
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
    let btn_area = chunks[3];
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
                .border_type(BorderType::Rounded)
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
                .border_type(BorderType::Rounded)
                .border_style(if is_cancel_focused {
                    Style::default().fg(active_color)
                } else {
                    Style::default().fg(border_color)
                }),
        )
        .centered()
        .render(btn_chunks[1], buf);
}
