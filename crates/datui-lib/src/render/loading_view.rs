//! The main view while a dataset is on its way in.
//!
//! It stands in for the table from the moment a load starts until that load installs
//! its dataset. Without it the previous dataset stayed on screen for the whole load —
//! one file's rows under another file's name, with only the control bar to say so.
//!
//! Drawn in the same family as the home screen: no boxes, centred, one accent. The
//! phase name is the progress indicator. It is a real, observable step ("Scanning
//! input", "Caching schema", "Loading buffer"), unlike the percentage beside it in
//! the control bar, which is a constant per phase.

use crate::glyphs;
use crate::render::context::RenderContext;
use crate::LoadingState;
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

    let (phase, path, size) = match &app.loading_state {
        LoadingState::Loading {
            current_phase,
            file_path,
            file_size,
            ..
        } => (current_phase.as_str(), file_path.clone(), *file_size),
        // A load with no phase of its own yet — the frame between the keypress and
        // the event that carries it out.
        _ => ("Loading", None, 0),
    };

    let g = glyphs::get();
    let frame = app.throbber_frame as usize % g.spinner.len();
    let mut lines = vec![
        Line::from(vec![
            Span::styled(
                format!("{}  ", g.spinner[frame]),
                Style::default().fg(ctx.throbber),
            ),
            Span::styled(
                format!("{phase}{}", g.ellipsis),
                Style::default()
                    .fg(ctx.text_primary)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(""),
    ];

    // The name first and the location under it: which file is coming is the question
    // being answered, and a long path would push the name off a narrow screen.
    if let Some(path) = path.as_ref() {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| crate::home::display_path(path));
        lines.push(Line::from(Span::styled(
            truncate(&name, area.width as usize),
            Style::default().fg(ctx.text_primary),
        )));
        let mut detail = crate::home::display_path(path);
        if size > 0 {
            detail = format!("{detail}   {}", crate::discover::format_size(size));
        }
        lines.push(Line::from(Span::styled(
            truncate_start(&detail, area.width as usize),
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

/// Keep the head of a string, marking what was cut.
fn truncate(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_string();
    }
    let g = glyphs::get();
    let keep = width.saturating_sub(g.ellipsis.chars().count());
    text.chars().take(keep).collect::<String>() + g.ellipsis
}

/// Keep the tail of a path; the leaf is what says where the file is.
fn truncate_start(text: &str, width: usize) -> String {
    let count = text.chars().count();
    if count <= width {
        return text.to_string();
    }
    let g = glyphs::get();
    let keep = width.saturating_sub(g.ellipsis.chars().count());
    let skip = count.saturating_sub(keep);
    g.ellipsis.to_string() + &text.chars().skip(skip).collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cells(buf: &Buffer) -> String {
        buf.content().iter().map(|c| c.symbol()).collect()
    }

    #[test]
    fn a_load_with_no_room_to_draw_is_skipped_rather_than_panicking() {
        // Terminals get resized to absurd sizes mid-load, and a load is exactly when
        // the user cannot press anything to recover.
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = crate::App::new(tx, test_runtime());
        app.set_loading_phase("Scanning input", 10);
        for (w, h) in [(1, 1), (4, 2), (11, 40), (200, 1)] {
            let area = Rect::new(0, 0, w, h);
            let mut buf = Buffer::empty(area);
            render(area, &mut buf, &app, &RenderContext::for_test());
        }
    }

    #[test]
    fn the_phase_and_the_file_are_both_on_screen() {
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = crate::App::new(tx, test_runtime());
        app.loading_state = LoadingState::Loading {
            file_path: Some(std::path::PathBuf::from("/tmp/quarterly.parquet")),
            file_size: 2048,
            current_phase: "Caching schema".to_string(),
            progress_percent: 40,
        };

        let area = Rect::new(0, 0, 60, 20);
        let mut buf = Buffer::empty(area);
        render(area, &mut buf, &app, &RenderContext::for_test());

        let text = cells(&buf);
        assert!(text.contains("Caching schema"), "phase missing: {text:?}");
        assert!(text.contains("quarterly.parquet"), "file missing: {text:?}");
        assert!(text.contains("2.0 KB"), "size missing: {text:?}");
    }

    fn test_runtime() -> tokio::runtime::Handle {
        static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
        RT.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("test runtime")
        })
        .handle()
        .clone()
    }
}
