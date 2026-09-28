//! Overlay rendering (confirmation/error modals, help).

use crate::render::context::RenderContext;
use crate::render::layout::centered_rect;
use crate::widgets::ui::{HintBar, Surface};
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::Widget;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

/// A compact centered popup sized by its message: as wide as the message wants
/// up to `max_width`, as tall as the wrapped text plus the frame and footer,
/// never past three quarters of the screen. A dialog is a commitment, and it
/// stays small rather than scaling with the terminal.
fn message_popup(area: Rect, message: &str, extra_rows: u16, max_width: u16) -> Rect {
    let width = max_width
        .min(area.width.saturating_sub(4))
        .max(area.width.min(30));
    // Frame (2) plus gutters (2) around the text.
    let inner = width.saturating_sub(4).max(1) as usize;
    let lines: usize = message
        .lines()
        .map(|l| l.chars().count().div_ceil(inner).max(1))
        .sum::<usize>()
        .max(1);
    // Text, a blank, any extra rows, the footer, and the frame.
    let height = (lines as u16 + extra_rows + 4)
        .min(area.height * 3 / 4)
        .max(6);
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    Rect::new(x, y, width.min(area.width), height.min(area.height))
}

/// Renders the confirmation modal: the question, a Yes/No choice the rail and
/// accent mark, and the keys in the footer. No buttons.
pub fn render_confirmation_modal(
    area: Rect,
    buf: &mut Buffer,
    modal: &crate::ConfirmationModal,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    let footer = HintBar::from_ctx(ctx)
        .hint("Enter", "Confirm")
        .hint(g.updown_lr, "Switch")
        .hint("Esc", "Cancel");
    let popup = message_popup(area, &modal.message, 2, 64);
    let content = Surface::new("Confirm")
        .footer(&footer)
        .border_style(Style::default().fg(ctx.modal_border_active))
        .render(popup, buf, ctx);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(content);

    Paragraph::new(modal.message.as_str())
        .style(Style::default().fg(ctx.text_primary))
        .wrap(ratatui::widgets::Wrap { trim: true })
        .render(rows[0], buf);

    // The choice the rail is on is the one Enter takes.
    let choice = |label: &str, focused: bool| -> Vec<Span<'static>> {
        let rail = if focused { g.rail } else { " " };
        let style = if focused {
            Style::default()
                .fg(ctx.accent_bright)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(ctx.text_secondary)
        };
        vec![
            Span::styled(rail.to_string(), Style::default().fg(ctx.accent)),
            Span::styled(label.to_string(), style),
        ]
    };
    let mut spans = choice(modal.yes_label, modal.focus_yes);
    spans.push(Span::raw("     "));
    spans.extend(choice("No", !modal.focus_yes));
    Paragraph::new(Line::from(spans)).render(rows[2], buf);
}

/// Renders the error modal: the message and the way out, nothing else.
pub fn render_error_modal(
    area: Rect,
    buf: &mut Buffer,
    modal: &crate::ErrorModal,
    ctx: &RenderContext,
) {
    let footer = HintBar::from_ctx(ctx)
        .hint("Enter", "OK")
        .hint("Esc", "Close");
    let popup = message_popup(area, &modal.message, 0, 64);
    let content = Surface::new("Error")
        .footer(&footer)
        .border_style(Style::default().fg(ctx.modal_border_error))
        .render(popup, buf, ctx);

    Paragraph::new(modal.message.as_str())
        .style(Style::default().fg(ctx.error))
        .wrap(ratatui::widgets::Wrap { trim: true })
        .render(content, buf);
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

    // The help files are written once, in Unicode; a terminal on the ASCII
    // floor gets the twins here, at the one boundary all of them cross.
    let text = crate::glyphs::asciify_instructions(text);

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

    fn grid(buf: &Buffer, area: Rect) -> Vec<String> {
        (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect()
    }

    /// One border, no bordered buttons: the frame's corners are the only ones.
    #[test]
    fn the_error_modal_is_one_surface_with_the_keys_in_the_footer() {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, 80, 24);
        let mut buf = Buffer::empty(area);
        let mut modal = crate::ErrorModal::new();
        modal.show("Select at least one index column.".to_string());
        render_error_modal(area, &mut buf, &modal, &ctx);
        let rows = grid(&buf, area);
        let corners: usize = rows.iter().map(|r| r.matches('╭').count()).sum();
        assert_eq!(corners, 1, "one frame, no inner boxes: {rows:#?}");
        let text = rows.join("\n");
        assert!(text.contains("Select at least one index column."));
        assert!(text.contains("Enter") && text.contains("OK") && text.contains("Esc"));
    }

    /// The focused choice carries the rail; there is nothing to Tab onto.
    #[test]
    fn the_confirmation_modal_marks_the_choice_with_the_rail() {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, 80, 24);
        let mut buf = Buffer::empty(area);
        let mut modal = crate::ConfirmationModal::new();
        modal.show("Overwrite out.csv?".to_string());
        render_confirmation_modal(area, &mut buf, &modal, &ctx);
        let rows = grid(&buf, area);
        let corners: usize = rows.iter().map(|r| r.matches('╭').count()).sum();
        assert_eq!(corners, 1, "one frame, no button boxes: {rows:#?}");
        let text = rows.join("\n");
        assert!(text.contains("Overwrite out.csv?"));
        let choice_row = rows
            .iter()
            .find(|r| r.contains("Yes") && r.contains("No"))
            .expect("the Yes/No line is there");
        assert!(
            choice_row.contains("▎Yes"),
            "the rail is on Yes by default: {choice_row:?}"
        );
        assert!(text.contains("Confirm") && text.contains("Cancel"));

        // Switching focus moves the rail, not the labels.
        modal.focus_yes = false;
        let mut buf2 = Buffer::empty(area);
        render_confirmation_modal(area, &mut buf2, &modal, &ctx);
        let rows2 = grid(&buf2, area);
        let choice_row2 = rows2
            .iter()
            .find(|r| r.contains("Yes") && r.contains("No"))
            .unwrap();
        assert!(choice_row2.contains("▎No"), "{choice_row2:?}");
        // Compare columns, not byte offsets: the rail glyph is multi-byte.
        let col = |r: &str| r.replace('\u{258e}', " ").find("Yes");
        assert_eq!(
            col(choice_row),
            col(choice_row2),
            "labels hold still while the rail moves"
        );
    }
}
