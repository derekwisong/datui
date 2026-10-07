//! The prompts the footer grows for: the command line (`:`) and find (`/`).
//!
//! Each is a line under the footer's status line: a prefix that says what Enter
//! will do (`row:`, `sql:`, `q:`, `/`), the text, and at the right what the find
//! lights up. A line under it says why the last run failed or, on the command
//! line, the columns the word at the cursor could name.

use crate::QueryMode;
use crate::render::context::RenderContext;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

/// What the empty SQL input shows, dimmed, until something is typed.
pub const SQL_PLACEHOLDER: &str = "SELECT * FROM df WHERE ...";

/// The most rows a statement takes in the footer.
const MAX_INPUT_ROWS: u16 = 2;

/// The prompt's prefix: what Enter does with the text.
pub fn prefix(app: &crate::App) -> &'static str {
    match app.prompt.input_type {
        Some(crate::InputType::Find) => "/",
        _ => {
            let text = app.query_prompt_text().unwrap_or_default();
            if crate::editing_keys::row_number(text).is_some() {
                "row:"
            } else {
                app.prompt.query_mode.prefix_colon()
            }
        }
    }
}

/// Why the prompt's last run failed, if it did.
pub fn error(app: &crate::App) -> Option<String> {
    match app.prompt.input_type {
        Some(crate::InputType::Find) => app.prompt.find.error.clone(),
        Some(crate::InputType::Query) => app.query_prompt_error(),
        None => None,
    }
}

/// Rows the prompt takes under the status line, at `width` columns, with at most
/// `room` of them.
pub fn rows(app: &crate::App, width: u16, room: u16) -> u16 {
    if app.input_mode != crate::InputMode::Editing || room == 0 {
        return 0;
    }
    let (input, below) = plan(app, width, room);
    input + below
}

/// The prompt's input rows and the rows under it, within `room`. A statement over
/// two lines keeps both before the column list does; an error keeps its line, and
/// the statement gives up its second.
fn plan(app: &crate::App, width: u16, room: u16) -> (u16, u16) {
    let input = match app.prompt.input_type {
        Some(crate::InputType::Query) if app.prompt.query_mode == QueryMode::Sql => {
            let text_width = width.saturating_sub(prefix_width(app) + 2);
            (app.prompt.sql_input.visual_rows(text_width) as u16).clamp(1, MAX_INPUT_ROWS)
        }
        _ => 1,
    };
    let error = error(app).is_some();
    let below = u16::from(error || columns_line(app));
    if input + below <= room {
        (input, below)
    } else if error {
        (room.saturating_sub(1).max(1), u16::from(room > 1))
    } else {
        (input.min(room), 0)
    }
}

/// Whether the command line lists columns under its input: while a query is being
/// typed, not a row number.
fn columns_line(app: &crate::App) -> bool {
    app.prompt.input_type == Some(crate::InputType::Query)
        && !app.prompt.sql_columns.is_empty()
        && crate::editing_keys::row_number(app.query_prompt_text().unwrap_or_default()).is_none()
}

fn prefix_width(app: &crate::App) -> u16 {
    crate::glyphs::display_width(prefix(app)) as u16 + 1
}

/// Draw the prompt into `area`: its input line or lines, then the error or the
/// column list.
pub fn render(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &crate::App,
    ctx: &RenderContext,
) {
    if area.height == 0 || area.width < 4 {
        return;
    }
    let error = error(app);
    let (input_rows, below) = plan(app, area.width, area.height);
    let input_rows = input_rows.min(area.height);
    let accent = Style::default()
        .fg(ctx.keybind_hints)
        .add_modifier(Modifier::BOLD);
    let prefix = prefix(app);
    let lead = prefix_width(app) + 1;
    Paragraph::new(Line::from(vec![
        Span::raw(" "),
        Span::styled(prefix, accent),
    ]))
    .render(
        Rect {
            height: 1,
            width: lead.min(area.width),
            ..area
        },
        buf,
    );

    // At the right of the find's line: how it matches, each switch beside its key
    // (lit while on), and what it lights up on screen.
    let mut right_width = 0u16;
    if app.prompt.input_type == Some(crate::InputType::Find) {
        let dim = Style::default().fg(ctx.dimmed);
        let on = Style::default().fg(ctx.text_primary);
        let column = app.prompt.find.column.as_deref().unwrap_or("column");
        let switches = [
            ("^R", "regex".to_string(), app.prompt.find.regex),
            ("^T", "letters".to_string(), app.prompt.find.fuzzy),
            ("^L", format!("in {column}"), app.prompt.find.in_column),
        ];
        let count = app.live_on_screen().map(|n| {
            Span::styled(
                format!("{} on screen", crate::numfmt::group_chrome(n)),
                Style::default().fg(ctx.text_secondary),
            )
        });
        let mut spans: Vec<Span> = Vec::new();
        for (key, word, lit) in switches {
            spans.push(Span::styled(key, accent));
            spans.push(Span::raw(" "));
            spans.push(Span::styled(word, if lit { on } else { dim }));
            spans.push(Span::raw("  "));
        }
        let mut line = Line::from(spans);
        line.spans.extend(count.clone());
        line.spans.push(Span::raw(" "));
        // The pattern keeps at least half the line; the switches go before the count.
        if line.width() as u16 >= area.width / 2 {
            line = Line::from(
                count
                    .into_iter()
                    .chain([Span::raw(" ")])
                    .collect::<Vec<_>>(),
            );
        }
        let w = line.width() as u16;
        if w < area.width / 2 {
            right_width = w;
            Paragraph::new(line).render(
                Rect {
                    x: area.right() - w,
                    width: w,
                    height: 1,
                    ..area
                },
                buf,
            );
        }
    }

    let input_area = Rect {
        x: area.x + lead.min(area.width),
        y: area.y,
        width: area
            .width
            .saturating_sub(lead + right_width + u16::from(right_width > 0)),
        height: input_rows,
    };
    match app.prompt.input_type {
        Some(crate::InputType::Find) => (&app.prompt.find.input).render(input_area, buf),
        _ => {
            let input = app.query_input_shown();
            input.render(input_area, buf);
            if app.prompt.query_mode == QueryMode::Sql && input.is_empty() {
                render_placeholder(input_area, buf, ctx, input.is_focused());
            }
        }
    }

    if below == 0 {
        return;
    }
    let line_area = Rect {
        x: area.x + 1,
        y: area.y + input_rows,
        width: area.width.saturating_sub(2),
        height: 1,
    };
    if let Some(error) = error {
        // One line: the first of the reason, cut with a mark.
        let first = error.lines().next().unwrap_or_default();
        let more = error.lines().count() > 1;
        let width = line_area.width as usize;
        let mut text = crate::glyphs::fit_cells(first, width, "...").into_owned();
        if more && crate::glyphs::display_width(&text) + 4 <= width && !text.ends_with("...") {
            text.push_str(" ...");
        }
        Paragraph::new(text)
            .style(Style::default().fg(ctx.error))
            .render(line_area, buf);
    } else {
        render_columns(app, line_area, buf, ctx);
    }
}

/// The example an empty SQL input shows, after the cursor.
fn render_placeholder(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    ctx: &RenderContext,
    focused: bool,
) {
    let skip = u16::from(focused);
    if area.width <= skip || area.height == 0 {
        return;
    }
    let area = Rect {
        x: area.x + skip,
        width: area.width - skip,
        height: 1,
        ..area
    };
    Paragraph::new(SQL_PLACEHOLDER)
        .style(Style::default().fg(ctx.dimmed))
        .render(area, buf);
}

/// The columns the word at the cursor could name, each in its type's color, on one
/// line. What does not fit is counted, not cut.
fn render_columns(
    app: &crate::App,
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    ctx: &RenderContext,
) {
    let matched = app.sql_column_matches();
    // A word that names no column is a keyword or a function: the whole list is
    // more use then than an empty one.
    let shown: Vec<&(String, polars::prelude::DataType)> = if matched.is_empty() {
        app.prompt.sql_columns.iter().collect()
    } else {
        matched
    };
    let g = crate::glyphs::get();
    let width = area.width as usize;
    let more = |n: usize| format!("{} {} more", g.ellipsis, n);
    let mut spans: Vec<Span> = Vec::new();
    let mut used = 0usize;
    let mut placed = 0usize;
    for (i, (name, _)) in shown.iter().enumerate() {
        let w = crate::glyphs::display_width(name);
        let gap = if used == 0 { 0 } else { 2 };
        let rest = shown.len() - i - 1;
        let tail = if rest > 0 {
            2 + crate::glyphs::display_width(&more(rest))
        } else {
            0
        };
        if used + gap + w + tail > width {
            break;
        }
        if gap > 0 {
            spans.push(Span::raw("  "));
        }
        spans.push(Span::styled(
            name.clone(),
            Style::default().fg(ctx.type_color(&shown[i].1)),
        ));
        used += gap + w;
        placed = i + 1;
    }
    if placed < shown.len() {
        if !spans.is_empty() {
            spans.push(Span::raw("  "));
        }
        spans.push(Span::styled(
            more(shown.len() - placed),
            Style::default().fg(ctx.dimmed),
        ));
    }
    Paragraph::new(Line::from(spans)).render(area, buf);
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::DataType;
    use ratatui::buffer::Buffer;

    fn draw(app: &mut crate::App, width: u16, height: u16) -> Vec<String> {
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        render(
            area,
            &mut buf,
            app,
            &crate::render::context::RenderContext::for_test(),
        );
        crate::tests::buffer_lines(&buf)
    }

    fn command_line(mode: QueryMode) -> crate::App {
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = crate::App::new(tx, crate::tests::test_runtime());
        app.input_mode = crate::InputMode::Editing;
        app.prompt.input_type = Some(crate::InputType::Query);
        app.prompt.query_mode = mode;
        app.prompt.sql_columns = vec![
            ("Round".to_string(), DataType::Int64),
            ("Date".to_string(), DataType::String),
            ("Team 1".to_string(), DataType::String),
            ("Team 2".to_string(), DataType::String),
            ("FT".to_string(), DataType::String),
        ];
        app
    }

    /// The prefix says what Enter will do: digits go to a row, anything else runs
    /// in the language the line is in.
    #[test]
    fn the_prefix_names_row_sql_or_q() {
        let mut app = command_line(QueryMode::Q);
        assert_eq!(prefix(&app), "q:");
        app.prompt.query_input.set_value("1200");
        assert_eq!(prefix(&app), "row:");
        app.prompt.query_input.set_value("1200 rows");
        assert_eq!(prefix(&app), "q:");
        #[cfg(feature = "sql")]
        {
            let app = command_line(QueryMode::Sql);
            assert_eq!(prefix(&app), "sql:");
        }
    }

    /// Under the input, the columns the word at the cursor could name.
    #[test]
    fn the_columns_narrow_to_the_word_typed() {
        let mut app = command_line(QueryMode::Q);
        app.prompt.query_input.set_value("select Te");
        assert_eq!(rows(&app, 80, 2), 2);
        let screen = draw(&mut app, 80, 2).join("\n");
        assert!(screen.contains("q: select Te"), "{screen}");
        assert!(
            screen.contains("Team 1") && screen.contains("Team 2"),
            "{screen}"
        );
        assert!(!screen.contains("Round"), "{screen}");
    }

    /// What does not fit is counted rather than cut.
    #[test]
    fn columns_that_do_not_fit_are_counted() {
        let mut app = command_line(QueryMode::Q);
        app.prompt.sql_columns = (0..40)
            .map(|i| (format!("measurement_{i}"), DataType::Float64))
            .collect();
        let screen = draw(&mut app, 60, 2).join("\n");
        assert!(screen.contains(" more"), "{screen}");
    }

    /// A row number lists no columns: the line is one row.
    #[test]
    fn a_row_number_takes_one_line() {
        let mut app = command_line(QueryMode::Q);
        app.prompt.query_input.set_value("42");
        assert_eq!(rows(&app, 80, 2), 1);
    }

    /// An empty SQL input shows where to start.
    #[cfg(feature = "sql")]
    #[test]
    fn an_empty_sql_input_shows_an_example() {
        let mut app = command_line(QueryMode::Sql);
        app.prompt.sql_input.set_focused(true);
        let screen = draw(&mut app, 80, 2).join("\n");
        assert!(screen.contains(SQL_PLACEHOLDER), "{screen}");
        app.prompt.sql_input.set_value("S");
        let screen = draw(&mut app, 80, 2).join("\n");
        assert!(!screen.contains(SQL_PLACEHOLDER), "{screen}");
    }
}
