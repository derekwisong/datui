//! Overlay rendering (confirmation/error modals, help).

use crate::render::context::RenderContext;
use crate::render::layout::{centered_rect, centered_rect_with_min};
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::Widget;
use ratatui::style::Style;
use ratatui::widgets::{Block, BorderType, Borders, Clear, Paragraph};

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
        .title_style(ratatui::style::Style::reset())
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

/// Renders the error modal (OK).
pub fn render_error_modal(
    area: Rect,
    buf: &mut Buffer,
    modal: &crate::ErrorModal,
    ctx: &RenderContext,
) {
    let popup_area = centered_rect(area, 70, 40);
    Clear.render(popup_area, buf);

    Block::default()
        .style(Style::default().bg(ctx.background))
        .render(popup_area, buf);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title("Error")
        .title_style(ratatui::style::Style::reset())
        .border_style(Style::default().fg(ctx.modal_border_error))
        .style(Style::default().bg(ctx.background));
    let inner_area = block.inner(popup_area);
    block.render(popup_area, buf);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(3)])
        .split(inner_area);

    Paragraph::new(modal.message.as_str())
        .style(Style::default().fg(ctx.error).bg(ctx.background))
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
/// Wrap one help line to `width` columns, breaking at word boundaries and
/// measuring characters, not bytes; a single overlong word is split hard.
fn wrap_help_line(line: &str, width: usize) -> Vec<String> {
    if width == 0 {
        return vec![String::new()];
    }
    let mut out = Vec::new();
    let mut current = String::new();
    let mut current_len = 0usize;
    for word in line.split(' ') {
        let word_len = word.chars().count();
        let sep = usize::from(current_len > 0);
        if current_len + sep + word_len <= width {
            if sep == 1 {
                current.push(' ');
            }
            current.push_str(word);
            current_len += sep + word_len;
            continue;
        }
        if current_len > 0 {
            out.push(std::mem::take(&mut current));
        }
        // A word wider than the line is split hard rather than lost.
        let mut rest: Vec<char> = word.chars().collect();
        while rest.len() > width {
            out.push(rest.drain(..width).collect());
        }
        current = rest.into_iter().collect();
        current_len = current.chars().count();
    }
    out.push(current);
    out
}

pub fn render_help_overlay(
    area: Rect,
    buf: &mut Buffer,
    title: &str,
    text: &str,
    scroll: &mut usize,
    ctx: &RenderContext,
) {
    // A reading surface: it caps its measure instead of stretching with an
    // ultrawide terminal.
    let mut popup_area = centered_rect(area, 80, 80);
    const MAX_MEASURE: u16 = 100;
    if popup_area.width > MAX_MEASURE {
        popup_area.x += (popup_area.width - MAX_MEASURE) / 2;
        popup_area.width = MAX_MEASURE;
    }
    Clear.render(popup_area, buf);

    let help_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Fill(1), Constraint::Length(1)])
        .split(popup_area);

    let text_area = help_layout[0];
    let scrollbar_area = help_layout[1];

    let footer = crate::widgets::ui::HintBar::from_ctx(ctx)
        .hint("Esc", "Close")
        .hint(crate::glyphs::get().updown, "Scroll");
    let inner_area = crate::widgets::ui::Surface::new(title)
        .footer(&footer)
        .render(text_area, buf, ctx);

    let available_width = inner_area.width as usize;
    let available_height = inner_area.height as usize;

    let mut wrapped_lines: Vec<String> = Vec::new();
    for line in text.lines() {
        if line.chars().count() <= available_width {
            wrapped_lines.push(line.to_string());
        } else {
            wrapped_lines.extend(wrap_help_line(line, available_width));
        }
    }

    let total_wrapped_lines = wrapped_lines.len();
    let max_scroll = total_wrapped_lines.saturating_sub(available_height);
    *scroll = (*scroll).min(max_scroll);
    let scroll_pos = *scroll;

    let visible_text = wrapped_lines
        .iter()
        .skip(scroll_pos)
        .take(available_height)
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    Paragraph::new(visible_text).render(inner_area, buf);

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
            buf.set_string(
                scrollbar_area.x,
                scrollbar_area.y + y,
                crate::glyphs::get().scroll_thumb,
                style,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn help_wrap_breaks_at_word_boundaries() {
        let wrapped = wrap_help_line("the quick brown fox jumps", 11);
        assert_eq!(wrapped, vec!["the quick", "brown fox", "jumps"]);
    }

    /// Width is characters, not bytes: a line of multibyte glyphs (arrows,
    /// box drawing) must not fold early or split inside a character.
    #[test]
    fn help_wrap_measures_characters_not_bytes() {
        let wrapped = wrap_help_line("↑↓ / j/k: Navigate", 18);
        assert_eq!(wrapped, vec!["↑↓ / j/k: Navigate"]);
    }

    #[test]
    fn an_overlong_word_is_split_rather_than_lost() {
        let wrapped = wrap_help_line("see /a/very/long/path/that/never/ends", 10);
        assert_eq!(wrapped.first().map(String::as_str), Some("see"));
        assert!(wrapped.iter().all(|line| line.chars().count() <= 10));
        assert_eq!(
            wrapped.join(""),
            "see/a/very/long/path/that/never/ends".replace(' ', "")
        );
    }

    /// The overlay says how to leave it, and caps its measure on wide
    /// terminals instead of stretching.
    #[test]
    fn the_overlay_names_esc_and_caps_its_measure() {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, 200, 40);
        let mut buf = Buffer::empty(area);
        let mut scroll = 0usize;
        render_help_overlay(
            area,
            &mut buf,
            "Table Help",
            "line one\nline two",
            &mut scroll,
            &ctx,
        );
        let rows: Vec<String> = (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect()
            })
            .collect();
        let text = rows.join("\n");
        assert!(
            text.contains("Esc") && text.contains("Close"),
            "the way out is named"
        );
        // The drawn region's width, wherever it is centered.
        let widest = rows
            .iter()
            .map(|row| {
                let trimmed = row.trim_end();
                let lead = trimmed.chars().take_while(|c| *c == ' ').count();
                trimmed.chars().count().saturating_sub(lead)
            })
            .max()
            .unwrap_or(0);
        assert!(
            widest <= 101,
            "the overlay stretched with the terminal: {widest} columns"
        );
    }
}
