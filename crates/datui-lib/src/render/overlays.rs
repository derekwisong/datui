//! Overlay rendering (confirmation/error modals, help).

use crate::render::context::RenderContext;
use crate::widgets::ui::{HintBar, Surface};
use datui_cli::keys::Context;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::Widget;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

/// A compact centered popup sized by its message: as wide as the message wants
/// up to `max_width`, as tall as the wrapped text plus the frame and footer,
/// never past three quarters of the screen. A dialog is a commitment, and it
/// stays small rather than scaling with the terminal.
fn message_popup(area: Rect, message: &str, extra_rows: u16, max_width: u16) -> Rect {
    let width = max_width
        .min(area.width.saturating_sub(4))
        .max(area.width.min(30));
    // Frame (2) plus gutters (2) around the text. Counted with the same
    // wrap the modal draws, in display columns, so the estimate cannot
    // disagree with the render.
    let inner = width.saturating_sub(4).max(1) as usize;
    let lines: usize = wrap_message(message, inner).len().max(1);
    // Text, a blank, any extra rows, the blank above the footer, the footer, and
    // the frame.
    let height = (lines as u16 + extra_rows + 5)
        .min(area.height * 3 / 4)
        .max(6);
    crate::render::layout::centered_rect(area, width, height)
}

/// The confirmation modal's keys: its footer, and the footer while it is up.
fn confirmation_keys() -> Vec<crate::render::footer::Hint> {
    ["Enter", "← / →", "Esc"]
        .into_iter()
        .map(|keys| crate::render::footer::registry_hint(Context::Question, keys))
        .collect()
}

/// Renders the confirmation modal: the question, a Yes/No choice the rail and
/// accent mark, and the keys in the footer. No buttons.
pub fn render_confirmation_modal(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut crate::ConfirmationModal,
    ctx: &RenderContext,
) {
    // A question owns the screen: a click outside it does nothing, and its own
    // footer keys, drawn after, take theirs.
    crate::app::pointer::record(area, crate::app::pointer::Hit::Modal);
    let g = crate::glyphs::get();
    let footer = confirmation_keys()
        .into_iter()
        .fold(HintBar::from_ctx(ctx), HintBar::push);
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

    render_scrollable_message(
        rows[0],
        buf,
        &modal.message,
        &mut modal.scroll,
        Style::default().fg(ctx.text_primary),
        ctx,
    );

    // The choice the rail is on is the one Enter takes.
    let choice = |label: &str, focused: bool| -> Vec<Span<'static>> {
        let rail = if focused { g.rail } else { " " };
        let style = if focused {
            Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
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
    spans.extend(choice(modal.no_label, !modal.focus_yes));
    Paragraph::new(Line::from(spans)).render(rows[2], buf);
}

/// Renders the error modal: the message and the way out, nothing else.
pub fn render_error_modal(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut crate::ErrorModal,
    ctx: &RenderContext,
) {
    crate::app::pointer::record(area, crate::app::pointer::Hit::Modal);
    // One way out, named once: Esc closes it too, as every dialog.
    let footer = HintBar::from_ctx(ctx)
        .screen(Context::Question)
        .key_as("Enter", "Close");
    let popup = message_popup(area, &modal.message, 0, 64);
    let content = Surface::new("Error")
        .footer(&footer)
        .border_style(Style::default().fg(ctx.modal_border_error))
        .render(popup, buf, ctx);

    render_scrollable_message(
        content,
        buf,
        &modal.message,
        &mut modal.scroll,
        Style::default().fg(ctx.error),
        ctx,
    );
}

/// One message, wrapped the way the height estimate counted it.
fn wrap_message(message: &str, inner: usize) -> Vec<String> {
    message
        .lines()
        .flat_map(|line| {
            if crate::glyphs::display_width(line) <= inner {
                vec![line.to_string()]
            } else {
                wrap_help_line(line, inner)
            }
        })
        .collect()
}

/// A message body that scrolls when its frame is capped: every character of
/// a long path or backend diagnostic stays reachable, and the last visible
/// row counts what is below rather than half-drawing it.
fn render_scrollable_message(
    area: Rect,
    buf: &mut Buffer,
    message: &str,
    scroll: &mut usize,
    style: Style,
    ctx: &RenderContext,
) {
    if area.height == 0 || area.width == 0 {
        return;
    }
    let lines = wrap_message(message, area.width as usize);
    let height = area.height as usize;
    let max_scroll = lines.len().saturating_sub(height);
    *scroll = (*scroll).min(max_scroll);
    let mut shown: Vec<String> = lines.iter().skip(*scroll).take(height).cloned().collect();
    let below = lines.len().saturating_sub(*scroll + height);
    if below > 0
        && let Some(last) = shown.last_mut()
    {
        let g = crate::glyphs::get();
        *last = format!(
            "{} {} more ({}/{})",
            g.ellipsis,
            below,
            *scroll + height,
            lines.len()
        );
    }
    Paragraph::new(shown.join("\n"))
        .style(style)
        .render(area, buf);
    let _ = ctx;
}

/// The fewest columns a wrapped help line's text keeps beside its hanging
/// indent; narrower than this, the indent gives way instead.
const MIN_HELP_MEASURE: usize = 16;

/// Wrap one help line to `width` display columns at word boundaries; a
/// single overlong word is split hard. Continuations hang under the text
/// they continue: a keyed row's under its description, a bullet's past its
/// dash, any other line under its own indent, so a wrapped row still reads
/// as one row of its table.
pub(crate) fn wrap_help_line(line: &str, width: usize) -> Vec<String> {
    use crate::glyphs::{display_width, take_columns};
    if width == 0 {
        return vec![String::new()];
    }
    let fits = |lead: usize| width.saturating_sub(lead) >= MIN_HELP_MEASURE;
    let mut indent = line.len() - line.trim_start_matches(' ').len();
    // A bullet's text hangs past its dash, as the help files lay one out.
    if line[indent..].starts_with("- ") {
        indent += 2;
    }
    let head_end = match crate::glyphs::key_gap(line) {
        Some((_, desc)) if fits(display_width(&line[..desc])) => desc,
        _ if fits(indent) => indent,
        _ => 0,
    };
    let (head, body) = line.split_at(head_end);
    let hang = display_width(head);
    let mut out = Vec::new();
    let mut row = head.to_string();
    let mut used = hang;
    let mut empty = true;
    // Runs of spaces inside the text survive as empty words, except where
    // they would start a row.
    for word in body.trim_start_matches(' ').split(' ') {
        if empty && word.is_empty() {
            continue;
        }
        let sep = usize::from(!empty);
        let word_width = display_width(word);
        if used + sep + word_width <= width {
            if sep == 1 {
                row.push(' ');
            }
            row.push_str(word);
            used += sep + word_width;
            empty = false;
            continue;
        }
        if !empty {
            out.push(row.trim_end().to_string());
            row = " ".repeat(hang);
            used = hang;
        }
        // A word wider than the line is split hard rather than lost, on a
        // character boundary measured in columns.
        let mut rest = word;
        while display_width(rest) > width - used {
            let cut = match take_columns(rest, width - used).len() {
                0 => rest.chars().next().map_or(rest.len(), char::len_utf8),
                n => n,
            };
            row.push_str(&rest[..cut]);
            out.push(std::mem::replace(&mut row, " ".repeat(hang)));
            used = hang;
            rest = &rest[cut..];
        }
        row.push_str(rest);
        used += display_width(rest);
        empty = rest.is_empty();
    }
    if !empty || out.is_empty() {
        out.push(row);
    }
    out
}

#[cfg(test)]
mod tests;
