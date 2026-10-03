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
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    Rect::new(x, y, width.min(area.width), height.min(area.height))
}

/// The confirmation modal's keys: its footer, and the control bar while it is up.
pub fn confirmation_keys() -> Vec<(&'static str, &'static str)> {
    vec![
        ("Enter", "Confirm"),
        (crate::glyphs::get().updown_lr, "Switch"),
        ("Esc", "Cancel"),
    ]
}

/// Renders the confirmation modal: the question, a Yes/No choice the rail and
/// accent mark, and the keys in the footer. No buttons.
pub fn render_confirmation_modal(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut crate::ConfirmationModal,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    let footer = confirmation_keys()
        .into_iter()
        .fold(HintBar::from_ctx(ctx), |bar, (key, label)| {
            bar.hint(key, label)
        });
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
    let footer = HintBar::from_ctx(ctx)
        .hint("Enter", "OK")
        .hint("Esc", "Close");
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
fn wrap_help_line(line: &str, width: usize) -> Vec<String> {
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

/// Renders the help overlay with wrapped text and scrollbar. Clamps and
/// updates `scroll` so the caller can persist it.
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
        if crate::glyphs::display_width(line) <= available_width {
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

    /// A keyed row hangs under its description, a bullet past its dash, a
    /// prose line at its indent; a column too narrow to hang in gives way to
    /// the indent.
    #[test]
    fn help_wrap_hangs_under_the_text_it_continues() {
        let row = "  Enter:      Open a finding, then its rows";
        assert_eq!(
            wrap_help_line(row, 32),
            [
                "  Enter:      Open a finding,",
                "              then its rows"
            ]
        );
        assert_eq!(
            wrap_help_line("  A note is an observation, not a fault", 24),
            ["  A note is an", "  observation, not a", "  fault"]
        );
        assert_eq!(
            wrap_help_line("  - Empty select: select (all columns)", 24),
            ["  - Empty select: select", "    (all columns)"]
        );
        assert_eq!(
            wrap_help_line(row, 24),
            ["  Enter:      Open a", "  finding, then its rows"]
        );
    }

    /// Every row of a help screen as the overlay lays it out in `area`, read
    /// off the buffer one scroll position at a time.
    /// The width the help overlay wraps its text to in `area`.
    fn help_text_width(area: Rect) -> usize {
        let mut popup = centered_rect(area, 80, 80);
        popup.width = popup.width.min(100);
        let text_area = Rect {
            width: popup.width.saturating_sub(1),
            ..popup
        };
        let mut scratch = Buffer::empty(area);
        let footer = crate::widgets::ui::HintBar::from_ctx(&RenderContext::for_test())
            .hint("Esc", "Close")
            .hint(crate::glyphs::get().updown, "Scroll");
        crate::widgets::ui::Surface::new("Help")
            .footer(&footer)
            .render(text_area, &mut scratch, &RenderContext::for_test())
            .width as usize
    }

    fn help_rows(area: Rect, text: &str) -> Vec<String> {
        let ctx = RenderContext::for_test();
        let draw = |scroll: usize| {
            let mut buf = Buffer::empty(area);
            let mut at = scroll;
            render_help_overlay(area, &mut buf, "Help", text, &mut at, &ctx);
            (at, grid(&buf, area))
        };
        // The first word is enough to find the text's corner.
        let first = text.split_whitespace().next().expect("a first word");
        let (_, top) = draw(0);
        let y0 = top
            .iter()
            .position(|r| r.contains(first))
            .expect("first word");
        let x0 = top[y0].find(first).expect("first word");
        let height = top[y0..]
            .iter()
            .position(|r| r.contains("Close"))
            .expect("the footer");
        // Every cell holds one character here, so columns are char counts.
        let x0 = top[y0][..x0].chars().count();
        let side = crate::glyphs::get().border.vertical_right;
        let right = top[y0]
            .chars()
            .collect::<Vec<_>>()
            .iter()
            .rposition(|c| side.starts_with(*c))
            .expect("frame");
        let cells = |row: &str| -> String {
            let text: String = row.chars().take(right).skip(x0).collect();
            text.trim_end().to_string()
        };
        let mut rows = Vec::new();
        let mut last = top;
        for scroll in 0.. {
            let (at, g) = draw(scroll);
            if at < scroll {
                break;
            }
            rows.push(cells(&g[y0]));
            last = g;
        }
        rows.extend(last[y0 + 1..y0 + height].iter().map(|r| cells(r)));
        rows
    }

    /// At 80×24 and 60×20, in UTF-8 and in ASCII, a wrapped help row
    /// continues under its description, a bullet past its dash, a prose line
    /// under its own indent, and no word goes missing.
    #[test]
    fn wrapped_help_rows_hang_under_their_description() {
        fn indent(line: &str) -> usize {
            line.len() - line.trim_start_matches(' ').len()
        }
        let screens = [
            crate::help_strings::main_view(),
            crate::help_strings::home(),
            crate::help_strings::sort_filter(),
            crate::help_strings::query(),
            crate::help_strings::analysis_data_quality(),
        ];
        let mut wrapped = 0;
        for (w, h) in [(80, 24), (60, 20)] {
            let area = Rect::new(0, 0, w, h);
            for help in screens {
                for text in [help.to_string(), crate::glyphs::instructions_in_ascii(help)] {
                    let rows = help_rows(area, &text);
                    let mut rows = rows.iter();
                    // What the overlay draws: ASCII twins when the locale is not UTF-8.
                    let drawn = crate::glyphs::asciify_instructions(&text);
                    for line in drawn.lines().map(str::trim_end) {
                        let first = rows.next().expect("a row per line");
                        assert!(line.starts_with(first.as_str()), "{first:?} for {line:?}");
                        let words: Vec<&str> = line.split_whitespace().collect();
                        let mut shown: Vec<&str> = first.split_whitespace().collect();
                        if shown.len() == words.len() {
                            continue;
                        }
                        let bullet = if line.trim_start().starts_with("- ") {
                            2
                        } else {
                            0
                        };
                        // As `wrap_help_line` decides: under the description while
                        // that leaves a readable measure, else under the indent.
                        let width = help_text_width(area);
                        let fits = |lead: usize| width.saturating_sub(lead) >= MIN_HELP_MEASURE;
                        let hang = match crate::glyphs::key_gap(line)
                            .map(|(_, desc)| crate::glyphs::display_width(&line[..desc]))
                        {
                            Some(desc) if fits(desc) => desc,
                            _ if fits(indent(line) + bullet) => indent(line) + bullet,
                            _ => 0,
                        };
                        while shown.len() < words.len() {
                            let next = rows.next().expect("the rest of a wrapped line");
                            assert_eq!(
                                indent(next),
                                hang,
                                "{w}x{h}: {next:?} does not hang under {line:?}"
                            );
                            shown.extend(next.split_whitespace());
                            wrapped += 1;
                        }
                        assert_eq!(shown, words, "{w}x{h}");
                    }
                }
            }
        }
        assert!(wrapped > 20, "long lines were wrapped: {wrapped}");
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

    /// A message taller than the capped frame scrolls: the first row is
    /// there at the top, the last is reachable at the bottom, and the cut
    /// row counts what is below instead of half-drawing it.
    #[test]
    fn a_long_error_scrolls_instead_of_hiding_its_tail() {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, 60, 20);
        let long = (0..40)
            .map(|i| format!("diagnostic line {i}"))
            .collect::<Vec<_>>()
            .join("\n");

        let mut modal = crate::ErrorModal::new();
        modal.show(long.clone());
        let mut buf = Buffer::empty(area);
        render_error_modal(area, &mut buf, &mut modal, &ctx);
        let text = grid(&buf, area).join("\n");
        assert!(text.contains("diagnostic line 0"), "{text}");
        assert!(text.contains("more"), "the cut says what is below: {text}");

        // Over-scrolling clamps, and the tail becomes reachable.
        modal.scroll = usize::MAX;
        let mut buf = Buffer::empty(area);
        render_error_modal(area, &mut buf, &mut modal, &ctx);
        let text = grid(&buf, area).join("\n");
        assert!(
            text.contains("diagnostic line 39"),
            "the last line is reachable: {text}"
        );
    }

    /// One border, no bordered buttons: the frame's corners are the only ones.
    #[test]
    fn the_error_modal_is_one_surface_with_the_keys_in_the_footer() {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, 80, 24);
        let mut buf = Buffer::empty(area);
        let mut modal = crate::ErrorModal::new();
        modal.show("Select at least one index column.".to_string());
        render_error_modal(area, &mut buf, &mut modal, &ctx);
        let rows = grid(&buf, area);
        let frames = crate::glyphs::frame_corners(&rows).len();
        assert_eq!(frames, 1, "one frame, no inner boxes: {rows:#?}");
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
        render_confirmation_modal(area, &mut buf, &mut modal, &ctx);
        let rows = grid(&buf, area);
        let frames = crate::glyphs::frame_corners(&rows).len();
        assert_eq!(frames, 1, "one frame, no button boxes: {rows:#?}");
        let text = rows.join("\n");
        assert!(text.contains("Overwrite out.csv?"));
        let choice_row = rows
            .iter()
            .find(|r| r.contains("Yes") && r.contains("No"))
            .expect("the Yes/No line is there");
        let rail = crate::glyphs::get().rail;
        assert!(
            choice_row.contains(&format!("{rail}Yes")),
            "the rail is on Yes by default: {choice_row:?}"
        );
        assert!(text.contains("Confirm") && text.contains("Cancel"));

        // Switching focus moves the rail, not the labels.
        modal.focus_yes = false;
        let mut buf2 = Buffer::empty(area);
        render_confirmation_modal(area, &mut buf2, &mut modal, &ctx);
        let rows2 = grid(&buf2, area);
        let choice_row2 = rows2
            .iter()
            .find(|r| r.contains("Yes") && r.contains("No"))
            .unwrap();
        assert!(
            choice_row2.contains(&format!("{rail}No")),
            "{choice_row2:?}"
        );
        // Compare columns, not byte offsets: the rail glyph is multi-byte.
        let col = |r: &str| r.replace(rail, " ").find("Yes");
        assert_eq!(
            col(choice_row),
            col(choice_row2),
            "labels hold still while the rail moves"
        );
    }
}
