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

use crate::LoadingState;
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
    // A folder of many files reads a footer from each before a row is shown, and on a
    // few thousand that is seconds of a screen saying only "Caching schema". The count
    // is what makes the wait legible: a number climbing is a wait, a number stopped is
    // a problem. `App::loading_phase` decides it for the control bar too, so the two
    // halves of the screen cannot say different things about one wait.
    let phase = app.loading_phase(phase);
    let phase = phase.as_ref();

    let g = glyphs::get();
    let frame = app.throbber_frame as usize % g.spinner.len();
    let mut lines = vec![
        Line::from(vec![
            Span::styled(
                format!("{}  ", g.spinner[frame]),
                Style::default().fg(ctx.throbber),
            ),
            Span::styled(
                // Truncated like the name and the location below it. The phase used to
                // be a couple of words and always fitted; "Reading footers: 1,203 of
                // 6,541" needs thirty-six columns, and cut by the terminal instead it
                // reads "of 6" — a smaller number than the one it is counting towards.
                truncate(
                    &format!("{phase}{}", g.ellipsis),
                    // The spinner and its two spaces come first on this line.
                    (area.width as usize).saturating_sub(3),
                ),
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

    /// While the footers are being read the screen counts them, and stops when they
    /// land.
    ///
    /// "Caching schema" is true of that wait but says nothing about its length; a
    /// folder of thousands of files spends seconds there. A number that climbs is a
    /// wait, and a number that stops is a problem — neither is legible without it.
    #[test]
    fn the_footer_count_replaces_the_phase_while_it_is_running() {
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = crate::App::new(tx, test_runtime());
        app.loading_state = LoadingState::Loading {
            file_path: Some(std::path::PathBuf::from("/tmp/blocks")),
            file_size: 2048,
            current_phase: "Caching schema".to_string(),
            progress_percent: 40,
        };
        let area = Rect::new(0, 0, 60, 20);
        let painted = |app: &crate::App| {
            let mut buf = Buffer::empty(area);
            render(area, &mut buf, app, &RenderContext::for_test());
            cells(&buf)
        };

        assert!(
            painted(&app).contains("Caching schema"),
            "the phase, while nothing is being counted"
        );

        app.footer_progress.begin(6541);
        for _ in 0..1203 {
            app.footer_progress.advance();
        }
        let text = painted(&app);
        assert!(
            text.contains("Reading footers: 1,203 of 6,541"),
            "the count, grouped so six thousand does not read as sixty: {text}"
        );
        assert!(
            !text.contains("Caching schema"),
            "and it replaces the phase rather than crowding in beside it: {text}"
        );

        app.footer_progress.done();
        assert!(
            painted(&app).contains("Caching schema"),
            "once they have landed there is no wait left to count"
        );
    }

    /// The count is cut with a mark at a width it does not fit, not by the terminal.
    ///
    /// "Caching schema" is fourteen characters and always fitted; "Reading footers:
    /// 1,203 of 6,541" needs thirty-six. Cut by the terminal instead of by the panel it
    /// ends mid-number — "of 6" where it means "of 6,541", a smaller figure than the one
    /// it is counting towards, which is the one way this line could actively mislead.
    #[test]
    fn the_footer_count_is_cut_with_a_mark_rather_than_by_the_edge() {
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = crate::App::new(tx, test_runtime());
        app.loading_state = LoadingState::Loading {
            file_path: Some(std::path::PathBuf::from("/tmp/blocks")),
            file_size: 2048,
            current_phase: "Caching schema".to_string(),
            progress_percent: 40,
        };
        app.footer_progress.begin(6541);
        for _ in 0..1203 {
            app.footer_progress.advance();
        }

        let ellipsis = crate::glyphs::get().ellipsis;
        for width in [12u16, 20, 24, 30, 36, 60] {
            let area = Rect::new(0, 0, width, 20);
            let mut buf = Buffer::empty(area);
            render(area, &mut buf, &app, &RenderContext::for_test());
            // The panel is centred, so the phase is not always on row zero.
            let line = (0..area.height)
                .map(|y| {
                    (0..area.width)
                        .map(|x| buf[(x, y)].symbol().to_string())
                        .collect::<String>()
                })
                .find(|row| row.contains("Read"))
                .map(|row| row.trim_end().to_string())
                .unwrap_or_else(|| panic!("no phase line at {width}"));
            assert!(
                line.chars().count() <= width as usize,
                "at {width} the line runs past the edge: {line:?}"
            );
            if width >= 36 {
                assert!(
                    line.contains("6,541"),
                    "at {width} the whole count fits: {line:?}"
                );
            } else {
                assert!(
                    line.ends_with(ellipsis),
                    "at {width} what is shown says it was cut: {line:?}"
                );
            }
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
