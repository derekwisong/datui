//! The query prompt and go-to-line strip.

use crate::QueryMode;
use crate::render::context::RenderContext;
use crate::widgets::ui::Surface;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Tabs, Widget};

/// What each mode takes, beside its tab.
fn mode_hint(mode: QueryMode) -> &'static str {
    match mode {
        QueryMode::Sql => "Table: df",
        QueryMode::Search => "Rows containing all words, in any text column",
        QueryMode::QStyle => "Subset of q, evaluated right to left; F1 syntax",
    }
}

/// Renders the input strip (the query prompt's modes, input and error, or the
/// go-to-line input) when in Editing mode.
pub fn render(
    input_area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &mut crate::App,
    has_error: bool,
    err_msg: &str,
    ctx: &RenderContext,
) {
    let title = match app.input_type {
        Some(crate::InputType::Search) => "Query",
        Some(crate::InputType::GoToLine) => "Go to line",
        None => "Input",
    };
    let mut surface = Surface::new(title);
    if has_error {
        surface = surface.border_style(Style::default().fg(ctx.modal_border_error));
    }
    let content = surface.render(input_area, buf, ctx);

    if app.input_type != Some(crate::InputType::Search) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Min(0),
            ])
            .split(content);
        (&app.query_input).render(chunks[0], buf);
        if has_error {
            render_error(err_msg, chunks[2], buf, ctx);
        }
        return;
    }

    let mode = app.query_mode;
    let tab_bar_focused = app.query_focus == crate::QueryFocus::TabBar;
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(0),
        ])
        .split(content);

    // The tabs name the current mode, so they are reserved in full; the hint
    // is the part that yields on a narrow strip.
    let g = crate::glyphs::get();
    let titles: Vec<&str> = QueryMode::available().iter().map(|m| m.title()).collect();
    let tabs_width = titles
        .iter()
        .map(|t| t.chars().count() as u16 + 2)
        .sum::<u16>()
        + (titles.len() as u16 - 1) * g.rule.chars().count() as u16;
    let tab_row = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(tabs_width), Constraint::Min(0)])
        .split(rows[0]);
    // The one tab style: the active tab carries the accent, bold.
    Tabs::new(titles)
        .divider(g.rule)
        .style(Style::default().fg(ctx.text_secondary))
        .highlight_style(
            Style::default()
                .fg(ctx.modal_border_active)
                .add_modifier(Modifier::BOLD),
        )
        .select(mode.index())
        .render(tab_row[0], buf);
    // Dropped whole rather than clipped: a cut-off syntax hint reads as wrong
    // syntax.
    let hint = mode_hint(mode);
    if hint.chars().count() as u16 <= tab_row[1].width {
        Paragraph::new(hint)
            .style(Style::default().fg(ctx.text_secondary))
            .alignment(Alignment::Right)
            .render(tab_row[1], buf);
    }

    let count = (mode == QueryMode::Search)
        .then(|| app.search_match_count())
        .flatten()
        .map(|n| match n {
            1 => "1 match".to_string(),
            n => format!("{} matches", crate::numfmt::group_chrome(n)),
        });
    render_rule(rows[1], buf, ctx, tab_bar_focused, count.as_deref());

    let input = match mode {
        QueryMode::Sql => &app.sql_input,
        QueryMode::Search => &app.fuzzy_input,
        QueryMode::QStyle => &app.query_input,
    };
    input.render(rows[2], buf);
    if has_error {
        render_error(err_msg, rows[3], buf, ctx);
    }
}

/// The line under the tabs: heavier and accented while the tab bar has focus,
/// with a flat chip at its right end for a count.
fn render_rule(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    ctx: &RenderContext,
    focused: bool,
    chip: Option<&str>,
) {
    let g = crate::glyphs::get();
    let (glyph, color) = if focused {
        (g.rule_h_focused, ctx.modal_border_active)
    } else {
        (g.rule_h, ctx.modal_border)
    };
    let width = area.width as usize;
    let chip = chip
        .map(|c| format!(" {c} "))
        .filter(|c| c.chars().count() + 2 <= width);
    let chip_w = chip.as_ref().map_or(0, |c| c.chars().count() + 1);
    let mut spans = vec![Span::styled(
        glyph.repeat(width - chip_w),
        Style::default().fg(color),
    )];
    if let Some(chip) = chip {
        spans.push(Span::styled(
            chip,
            Style::default().bg(ctx.controls_bg).fg(ctx.text_primary),
        ));
        spans.push(Span::styled(glyph, Style::default().fg(color)));
    }
    Paragraph::new(Line::from(spans)).render(area, buf);
}

fn render_error(err_msg: &str, area: Rect, buf: &mut ratatui::buffer::Buffer, ctx: &RenderContext) {
    Paragraph::new(err_msg)
        .style(Style::default().fg(ctx.error))
        .wrap(ratatui::widgets::Wrap { trim: true })
        .render(area, buf);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::buffer::Buffer;

    fn rows(buf: &Buffer) -> Vec<String> {
        let area = buf.area;
        (area.y..area.y + area.height)
            .map(|y| {
                (area.x..area.x + area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect()
            })
            .collect()
    }

    fn draw(app: &mut crate::App, width: u16) -> Vec<String> {
        let area = Rect::new(0, 0, width, 5);
        let mut buf = Buffer::empty(area);
        render(
            area,
            &mut buf,
            app,
            false,
            "",
            &crate::render::context::RenderContext::for_test(),
        );
        rows(&buf)
    }

    fn prompt() -> crate::App {
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = crate::App::new(tx, crate::tests::test_runtime());
        app.input_type = Some(crate::InputType::Search);
        app
    }

    /// The tabs name the current input mode; the hint is the part that
    /// yields. At 60 columns SQL used to lose its name.
    #[test]
    fn the_tab_names_survive_a_narrow_strip() {
        let mut app = prompt();
        for width in [40u16, 60, 80] {
            let painted = draw(&mut app, width).concat();
            for mode in QueryMode::available() {
                assert!(
                    painted.contains(mode.title()),
                    "{} lost at {width} columns",
                    mode.title()
                );
            }
        }
        // At 40 columns the hint has no room and is dropped whole, never
        // clipped mid-sentence.
        app.query_mode = QueryMode::QStyle;
        assert!(!draw(&mut app, 40).concat().contains("right to left"));
        assert!(draw(&mut app, 80).concat().contains("right to left"));
    }

    /// SQL comes first, and the tabs read in the documented order.
    #[cfg(feature = "sql")]
    #[test]
    fn the_modes_read_sql_search_q_style() {
        let mut app = prompt();
        let tab_row = &draw(&mut app, 80)[1];
        let sql = tab_row.find("SQL").unwrap();
        let search = tab_row.find("Search").unwrap();
        let q = tab_row.find("q-style").unwrap();
        assert!(sql < search && search < q, "{tab_row:?}");
    }

    /// One border: the Surface's frame, nothing box-drawn inside it.
    #[test]
    fn the_strip_is_one_surface() {
        let mut app = prompt();
        let rows = draw(&mut app, 80);
        assert!(
            rows[0].contains("Query"),
            "title on the frame: {:?}",
            rows[0]
        );
        for row in &rows[1..4] {
            assert!(!row.contains('╭') && !row.contains('╰'), "{row:?}");
        }
    }
}
