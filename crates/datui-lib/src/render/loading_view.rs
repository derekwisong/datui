//! The main view while a dataset loads, standing in for the table from load start to
//! install, so the previous dataset's rows never show under the new name. Home's style
//! (no boxes, centered, one accent); the phase name ("Scanning input", "Reading
//! schema", "Loading buffer") is the progress indicator.

use std::path::Path;

use crate::glyphs;
use crate::render::context::RenderContext;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph, Widget};

pub fn render(area: Rect, buf: &mut Buffer, app: &crate::App, ctx: &RenderContext) {
    Clear.render(area, buf);
    if area.width < 12 || area.height < 3 {
        return;
    }

    let (phase, path, size) = match app.load_shown() {
        Some((phase, _, path, size)) => (phase, path.map(Path::to_path_buf), size),
        _ => ("Loading", None, 0),
    };
    // The footer count replaces the phase while footers are read (a climbing number shows
    // progress); `App::loading_phase` decides it for the footer too, so both agree.
    let phase = app.loading_phase(phase);
    let phase = phase.as_ref();

    let g = glyphs::get();
    let frame = app.throbber_frame as usize % g.spinner.len();
    // Paused on the download confirmation: nothing is in progress, so no spinner and
    // no phase. The line stays so the file below it does not move.
    let phase_line = if app.awaiting_open_confirmation() {
        Line::from("")
    } else {
        Line::from(vec![
            Span::styled(
                format!("{}  ", g.spinner[frame]),
                Style::default().fg(ctx.throbber),
            ),
            Span::styled(
                // Truncated like the name and location: cut by the terminal, "Reading footers: 1,203 of
                // 6,541" could read "of 6".
                glyphs::fit(
                    &format!("{phase}{}", g.ellipsis),
                    // The spinner and its two spaces come first on this line.
                    (area.width as usize).saturating_sub(3),
                ),
                Style::default()
                    .fg(ctx.text_primary)
                    .add_modifier(Modifier::BOLD),
            ),
        ])
    };
    let mut lines = vec![phase_line, Line::from("")];

    // The name first and the location under it: which file is coming is the question
    // being answered, and a long path would push the name off a narrow screen.
    if let Some(path) = path.as_ref() {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| crate::home::display_path(path));
        lines.push(Line::from(Span::styled(
            glyphs::fit(&name, area.width as usize),
            Style::default().fg(ctx.text_primary),
        )));
        let mut detail = crate::home::display_path(path);
        if size > 0 {
            detail = format!("{detail}   {}", crate::numfmt::bytes(size));
        }
        lines.push(Line::from(Span::styled(
            glyphs::fit_start(&detail, area.width as usize),
            Style::default().fg(ctx.text_secondary),
        )));
    }

    // Centred vertically, a little above the middle: text sitting dead centre in a
    // tall terminal reads as low.
    let top = area
        .y
        .saturating_add(area.height.saturating_sub(lines.len() as u16) / 2)
        .saturating_sub(1)
        .max(area.y);
    let height = (lines.len() as u16).min(area.height.saturating_sub(top - area.y));
    let body = Rect {
        x: area.x,
        y: top,
        width: area.width,
        height,
    };
    Paragraph::new(lines).centered().render(body, buf);
}

#[cfg(test)]
mod tests;
