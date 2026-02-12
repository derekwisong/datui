//! Chart export modal rendering: format (vertical radio), path, title, dimensions, buttons (right).

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

    // Left: format list. Right: path, title, width x height, buttons.
    let horz = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(12), Constraint::Min(40)])
        .split(inner);

    let format_list_area = horz[0];
    let format_selected = ChartExportFormat::ALL
        .iter()
        .position(|&f| f == modal.selected_format)
        .unwrap_or(0);
    let is_format_focused = modal.focus == ChartExportFocus::FormatSelector;
    let format_labels: Vec<&str> = ChartExportFormat::ALL.iter().map(|f| f.as_str()).collect();
    RadioBlock::new(
        " Format ",
        &format_labels,
        format_selected,
        is_format_focused,
        1,
        border_color,
        active_color,
    )
    .render(format_list_area, buf);

    let right = horz[1];
    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // File path
            Constraint::Length(3), // Chart title
            Constraint::Length(3), // Width x Height
            Constraint::Length(1), // Spacer
            Constraint::Length(3), // Buttons
        ])
        .split(right);

    // File path (top)
    let path_area = right_chunks[0];
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

    // Chart title
    let title_area = right_chunks[1];
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

    // Width x Height row
    let size_area = right_chunks[2];
    let size_row = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(size_area);
    let is_width_focused = modal.focus == ChartExportFocus::WidthInput;
    let is_height_focused = modal.focus == ChartExportFocus::HeightInput;
    let width_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(if is_width_focused {
            active_color
        } else {
            border_color
        }))
        .title(" Width ");
    let width_inner = width_block.inner(size_row[0]);
    width_block.render(size_row[0], buf);
    modal.width_input.set_focused(is_width_focused);
    (&modal.width_input).render(width_inner, buf);
    let height_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(if is_height_focused {
            active_color
        } else {
            border_color
        }))
        .title(" Height ");
    let height_inner = height_block.inner(size_row[1]);
    height_block.render(size_row[1], buf);
    modal.height_input.set_focused(is_height_focused);
    (&modal.height_input).render(height_inner, buf);

    // Buttons
    let btn_area = right_chunks[4];
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
