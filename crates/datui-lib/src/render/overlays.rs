//! Overlay rendering (loading/export gauge, confirmation/success/error modals, help).

use crate::render::context::RenderContext;
use crate::render::layout::{centered_rect, centered_rect_with_min};
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::Widget;
use ratatui::style::Style;
use ratatui::widgets::{Block, BorderType, Borders, Clear, Gauge, Paragraph};

/// Renders a bordered box with progress gauge; border and gauge use the given colors.
pub fn render_loading_gauge(
    area: Rect,
    buf: &mut Buffer,
    title: &str,
    label: &str,
    progress_percent: u16,
    border_color: ratatui::style::Color,
    gauge_fill_color: ratatui::style::Color,
) {
    Clear.render(area, buf);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title(title)
        .border_style(Style::default().fg(border_color));

    let inner = block.inner(area);
    block.render(area, buf);

    let gauge = Gauge::default()
        .gauge_style(Style::default().fg(gauge_fill_color))
        .percent(progress_percent)
        .label(label);

    gauge.render(inner, buf);
}

/// Renders the confirmation modal (Yes/No).
pub fn render_confirmation_modal(
    area: Rect,
    buf: &mut Buffer,
    modal: &crate::ConfirmationModal,
    ctx: &RenderContext,
) {
    let popup_area = centered_rect_with_min(area, 64, 26, 50, 12);
    Clear.render(popup_area, buf);

    Block::default()
        .style(Style::default().bg(ctx.background))
        .render(popup_area, buf);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title("Confirm")
        .border_style(Style::default().fg(ctx.modal_border_active))
        .style(Style::default().bg(ctx.background));
    let inner_area = block.inner(popup_area);
    block.render(popup_area, buf);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(6), Constraint::Length(3)])
        .split(inner_area);

    Paragraph::new(modal.message.as_str())
        .style(Style::default().fg(ctx.text_primary).bg(ctx.background))
        .wrap(ratatui::widgets::Wrap { trim: true })
        .render(chunks[0], buf);

    let button_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Fill(1),
            Constraint::Length(12),
            Constraint::Length(2),
            Constraint::Length(12),
            Constraint::Fill(1),
        ])
        .split(chunks[1]);

    let yes_style = if modal.focus_yes {
        Style::default().fg(ctx.modal_border_active)
    } else {
        Style::default()
    };
    let no_style = if !modal.focus_yes {
        Style::default().fg(ctx.modal_border_active)
    } else {
        Style::default()
    };

    Paragraph::new("Yes")
        .centered()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(yes_style),
        )
        .render(button_chunks[1], buf);

    Paragraph::new("No")
        .centered()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(no_style),
        )
        .render(button_chunks[3], buf);
}

/// Renders the success modal (OK).
pub fn render_success_modal(
    area: Rect,
    buf: &mut Buffer,
    modal: &crate::SuccessModal,
    ctx: &RenderContext,
) {
    let popup_area = centered_rect(area, 70, 40);
    Clear.render(popup_area, buf);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title("Success");
    let inner_area = block.inner(popup_area);
    block.render(popup_area, buf);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(3)])
        .split(inner_area);

    Paragraph::new(modal.message.as_str())
        .style(Style::default().fg(ctx.text_primary))
        .wrap(ratatui::widgets::Wrap { trim: true })
        .render(chunks[0], buf);

    let ok_style = Style::default().fg(ctx.modal_border_active);
    Paragraph::new("OK")
        .centered()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(ok_style),
        )
        .render(chunks[1], buf);
}

/// Renders the error modal (OK).
pub fn render_error_modal(
    area: Rect,
    buf: &mut Buffer,
    modal: &crate::ErrorModal,
    ctx: &RenderContext,
) {
    let popup_area = centered_rect(area, 70, 40);
    Clear.render(popup_area, buf);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title("Error")
        .border_style(Style::default().fg(ctx.modal_border_error));
    let inner_area = block.inner(popup_area);
    block.render(popup_area, buf);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(3)])
        .split(inner_area);

    Paragraph::new(modal.message.as_str())
        .style(Style::default().fg(ctx.error))
        .wrap(ratatui::widgets::Wrap { trim: true })
        .render(chunks[0], buf);

    let ok_style = Style::default().fg(ctx.modal_border_active);
    Paragraph::new("OK")
        .centered()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(ok_style),
        )
        .render(chunks[1], buf);
}

/// Renders the help overlay with wrapped text and scrollbar. Clamps and updates `scroll` so the caller can persist it.
pub fn render_help_overlay(
    area: Rect,
    buf: &mut Buffer,
    title: &str,
    text: &str,
    scroll: &mut usize,
    ctx: &RenderContext,
) {
    let popup_area = centered_rect(area, 80, 80);
    Clear.render(popup_area, buf);

    let help_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Fill(1), Constraint::Length(1)])
        .split(popup_area);

    let text_area = help_layout[0];
    let scrollbar_area = help_layout[1];

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded);
    let inner_area = block.inner(text_area);
    block.render(text_area, buf);

    let text_lines: Vec<&str> = text.lines().collect();
    let available_width = inner_area.width as usize;
    let available_height = inner_area.height as usize;

    let mut wrapped_lines = Vec::new();
    for line in &text_lines {
        if line.len() <= available_width {
            wrapped_lines.push(*line);
        } else {
            let mut remaining = *line;
            while !remaining.is_empty() {
                let mut take = remaining.len().min(available_width);
                while take > 0 && !remaining.is_char_boundary(take) {
                    take -= 1;
                }
                let take_len = if take == 0 {
                    remaining.chars().next().map_or(0, |c| c.len_utf8())
                } else {
                    take
                };
                let (chunk, rest) = remaining.split_at(take_len);
                wrapped_lines.push(chunk);
                remaining = rest;
            }
        }
    }

    let total_wrapped_lines = wrapped_lines.len();
    let max_scroll = total_wrapped_lines.saturating_sub(available_height).max(0);
    *scroll = (*scroll).min(max_scroll);
    let scroll_pos = *scroll;

    let visible_lines: Vec<&str> = wrapped_lines
        .iter()
        .skip(scroll_pos)
        .take(available_height)
        .copied()
        .collect();

    let visible_text = visible_lines.join("\n");
    Paragraph::new(visible_text)
        .wrap(ratatui::widgets::Wrap { trim: false })
        .render(inner_area, buf);

    if total_wrapped_lines > available_height {
        let scrollbar_height = scrollbar_area.height;
        let scrollbar_pos = if max_scroll > 0 {
            ((scroll_pos as f64 / max_scroll as f64) * (scrollbar_height.saturating_sub(1) as f64))
                as u16
        } else {
            0
        };

        let thumb_size = ((available_height as f64 / total_wrapped_lines as f64)
            * scrollbar_height as f64)
            .max(1.0) as u16;
        let thumb_size = thumb_size.min(scrollbar_height);

        for y in 0..scrollbar_height {
            let is_thumb = y >= scrollbar_pos && y < scrollbar_pos + thumb_size;
            let style = if is_thumb {
                Style::default().bg(ctx.text_primary)
            } else {
                Style::default().bg(ctx.surface)
            };
            buf.set_string(scrollbar_area.x, scrollbar_area.y + y, "█", style);
        }
    }
}
