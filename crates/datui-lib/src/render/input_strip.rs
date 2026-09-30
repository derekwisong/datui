//! The query prompt and go-to-line strip.

use crate::QueryMode;
use crate::render::context::RenderContext;
use crate::widgets::ui::{SectionRule, Surface};
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Tabs, Widget};

/// What each mode takes, beside its tab.
fn mode_hint(mode: QueryMode) -> &'static str {
    match mode {
        QueryMode::Sql => "Table: df; Alt+Enter for a new line",
        QueryMode::Search => "Every word's letters in order, in any text column",
        QueryMode::QStyle => "Subset of q, evaluated right to left; F1 syntax",
    }
}

/// What the empty SQL input shows, dimmed, until something is typed.
pub const SQL_PLACEHOLDER: &str = "SELECT * FROM df WHERE ...";

/// Rows a long statement may take before it scrolls within them.
const MAX_INPUT_ROWS: u16 = 4;
/// Rows the reason for a failed run may take.
const MAX_ERROR_ROWS: u16 = 4;
/// Rows of the column list under a SQL statement.
const COLUMN_ROWS: u16 = 2;
/// The frame, the tab row and the rule under it.
const CHROME_ROWS: u16 = 4;

/// How the query prompt shares its rows out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StripRows {
    pub input: u16,
    pub error: u16,
    /// Rows of column names under the Columns rule; zero drops the section.
    pub columns: u16,
}

impl StripRows {
    pub fn total(&self) -> u16 {
        let columns = if self.columns > 0 {
            self.columns + 1
        } else {
            0
        };
        CHROME_ROWS + self.input + self.error + columns
    }
}

/// Text width inside the strip: the frame and a gutter each side.
fn content_width(width: u16) -> u16 {
    width.saturating_sub(4)
}

/// The query prompt's rows at `width` columns, within `room` rows. As room runs
/// out the column list goes first, then the error and the statement give up rows,
/// never below one each.
pub fn plan(app: &crate::App, width: u16, room: u16, error: Option<&str>) -> StripRows {
    let text_width = content_width(width);
    let sql = app.query_mode == QueryMode::Sql;
    let input = if sql {
        (app.sql_input.visual_rows(text_width) as u16).clamp(1, MAX_INPUT_ROWS)
    } else {
        1
    };
    let error = error.map_or(0, |e| {
        (wrap_words(e, text_width as usize).len() as u16).clamp(1, MAX_ERROR_ROWS)
    });
    let columns = if sql && !app.sql_columns.is_empty() {
        COLUMN_ROWS
    } else {
        0
    };
    let mut rows = StripRows {
        input,
        error,
        columns,
    };
    let steps: [fn(&mut StripRows); 5] = [
        |r| r.columns = r.columns.min(1),
        |r| r.error = r.error.min(2),
        |r| r.input = r.input.min(2),
        |r| r.columns = 0,
        |r| {
            r.input = r.input.min(1);
            r.error = r.error.min(1);
        },
    ];
    for step in steps {
        if rows.total() <= room {
            break;
        }
        step(&mut rows);
    }
    rows
}

/// Renders the input strip (the query prompt's modes, input and error, or the
/// go-to-line input) when in Editing mode.
pub fn render(
    input_area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &mut crate::App,
    error: Option<&str>,
    ctx: &RenderContext,
) {
    let title = match app.input_type {
        Some(crate::InputType::Search) => "Query",
        Some(crate::InputType::GoToLine) => "Go to line",
        None => "Input",
    };
    let mut surface = Surface::new(title);
    if error.is_some() {
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
        if let Some(error) = error {
            render_error(error, chunks[2], buf, ctx);
        }
        return;
    }

    let mode = app.query_mode;
    let tab_bar_focused = app.query_focus == crate::QueryFocus::TabBar;
    let plan = plan(app, input_area.width, input_area.height, error);
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(plan.input),
            Constraint::Length(plan.error),
            Constraint::Length(if plan.columns > 0 { 1 } else { 0 }),
            Constraint::Length(plan.columns),
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
    if mode == QueryMode::Sql && input.is_empty() {
        render_placeholder(rows[2], buf, ctx, input.is_focused());
    }
    if let Some(error) = error {
        render_error(error, rows[3], buf, ctx);
    }
    if plan.columns > 0 {
        render_columns(app, rows[4], rows[5], buf, ctx);
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

/// The columns of `df` the word at the cursor could name, each in its type's
/// color with the type beside it, flowed across the rows given. What does not
/// fit is counted, not cut.
fn render_columns(
    app: &crate::App,
    rule: Rect,
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    ctx: &RenderContext,
) {
    let matched = app.sql_column_matches();
    // A word that names no column is a keyword or a function: the whole list
    // is more use then than an empty one.
    let shown: Vec<&(String, polars::prelude::DataType)> = if matched.is_empty() {
        app.sql_columns.iter().collect()
    } else {
        matched
    };
    let count = crate::numfmt::group_chrome(shown.len());
    SectionRule {
        title: "Columns",
        chip: Some(&count),
        focused: false,
    }
    .render(rule, buf, ctx);

    let g = crate::glyphs::get();
    let width = area.width as usize;
    let items: Vec<(String, String)> = shown
        .iter()
        .map(|(name, dtype)| (name.clone(), crate::widgets::datatable::dtype_label(dtype)))
        .collect();
    let item_width = |(name, label): &(String, String)| {
        crate::glyphs::display_width(name) + 1 + crate::glyphs::display_width(label)
    };
    // Items per row, then the last row gives items back until the count of
    // the rest fits beside it.
    let mut rows: Vec<Vec<usize>> = vec![Vec::new()];
    let mut used = 0;
    let mut placed = 0;
    for (i, item) in items.iter().enumerate() {
        let w = item_width(item);
        let gap = if used == 0 { 0 } else { 2 };
        if used > 0 && used + gap + w > width {
            if rows.len() == area.height as usize {
                break;
            }
            rows.push(Vec::new());
            used = 0;
        }
        let gap = if used == 0 { 0 } else { 2 };
        rows.last_mut().expect("a row").push(i);
        used += gap + w;
        placed = i + 1;
    }
    let more = |n: usize| format!("{} {} more", g.ellipsis, n);
    if placed < items.len() {
        let last = rows.last_mut().expect("a row");
        loop {
            let row_width: usize = last.iter().map(|&i| item_width(&items[i])).sum::<usize>()
                + 2 * last.len().saturating_sub(1);
            let tail = crate::glyphs::display_width(&more(items.len() - placed));
            if last.is_empty() || row_width + 2 + tail <= width {
                break;
            }
            last.pop();
            placed -= 1;
        }
    }
    let last_row = rows.len() - 1;
    for (r, row) in rows.iter().enumerate() {
        let mut spans = Vec::new();
        for (k, &i) in row.iter().enumerate() {
            if k > 0 {
                spans.push(Span::raw("  "));
            }
            let (name, label) = &items[i];
            spans.push(Span::styled(
                name.clone(),
                Style::default().fg(ctx.type_color(&shown[i].1)),
            ));
            spans.push(Span::raw(" "));
            spans.push(Span::styled(label.clone(), Style::default().fg(ctx.dimmed)));
        }
        if r == last_row && placed < items.len() {
            if !row.is_empty() {
                spans.push(Span::raw("  "));
            }
            spans.push(Span::styled(
                more(items.len() - placed),
                Style::default().fg(ctx.dimmed),
            ));
        }
        let line_area = Rect {
            y: area.y + r as u16,
            height: 1,
            ..area
        };
        Paragraph::new(Line::from(spans)).render(line_area, buf);
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

/// Break `text` into lines of at most `width` columns, at spaces where it can.
/// Its own line breaks are kept.
pub fn wrap_words(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut lines = Vec::new();
    for paragraph in text.lines() {
        let mut line = String::new();
        for word in paragraph.split(' ') {
            let mut word = word.to_string();
            loop {
                let used = crate::glyphs::display_width(&line);
                let need = crate::glyphs::display_width(&word);
                let gap = usize::from(!line.is_empty());
                if used + gap + need <= width {
                    if gap == 1 {
                        line.push(' ');
                    }
                    line.push_str(&word);
                    break;
                }
                if !line.is_empty() {
                    lines.push(std::mem::take(&mut line));
                    continue;
                }
                // A word wider than the line is split where the line ends; a
                // character wider than the whole line goes on one of its own.
                let mut head = crate::glyphs::take_columns(&word, width).to_string();
                if head.is_empty() {
                    head = word.chars().next().map(String::from).unwrap_or_default();
                }
                word = word[head.len()..].to_string();
                if word.is_empty() {
                    line = head;
                    break;
                }
                lines.push(head);
            }
        }
        lines.push(line);
    }
    lines
}

fn render_error(err_msg: &str, area: Rect, buf: &mut ratatui::buffer::Buffer, ctx: &RenderContext) {
    let lines: Vec<Line> = wrap_words(err_msg, area.width as usize)
        .into_iter()
        .map(Line::from)
        .collect();
    Paragraph::new(lines)
        .style(Style::default().fg(ctx.error))
        .render(area, buf);
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::DataType;
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

    fn draw_sized(
        app: &mut crate::App,
        width: u16,
        height: u16,
        error: Option<&str>,
    ) -> Vec<String> {
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        render(
            area,
            &mut buf,
            app,
            error,
            &crate::render::context::RenderContext::for_test(),
        );
        rows(&buf)
    }

    fn draw(app: &mut crate::App, width: u16) -> Vec<String> {
        draw_sized(app, width, 5, None)
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

    fn sql_prompt() -> crate::App {
        let mut app = prompt();
        app.query_mode = QueryMode::Sql;
        app.sql_input.set_focused(true);
        app.sql_columns = vec![
            ("Round".to_string(), DataType::Int64),
            ("Date".to_string(), DataType::String),
            ("Team 1".to_string(), DataType::String),
            ("Team 2".to_string(), DataType::String),
            ("FT".to_string(), DataType::String),
        ];
        app
    }

    /// The Premier League goals query from the docs, about 220 characters.
    const GOALS: &str = "SELECT Round, STRPTIME(SUBSTR(Date, 1, 15), '%a %b %d %Y') AS \
        match_date, \"Team 1\" AS home, \"Team 2\" AS away, CAST(SPLIT_PART(FT, '–', 1) AS INT) \
        + CAST(SPLIT_PART(FT, '–', 2) AS INT) AS goals FROM df ORDER BY goals DESC";

    /// A long statement wraps onto rows of its own at 80 columns: every word of
    /// it is on screen, and nothing scrolls sideways.
    #[cfg(feature = "sql")]
    #[test]
    fn a_long_statement_wraps_instead_of_scrolling() {
        let mut app = sql_prompt();
        app.sql_input.set_value(GOALS);
        let plan = plan(&app, 80, 17, None);
        assert!(plan.input >= 3, "{plan:?}");
        let screen = draw_sized(&mut app, 80, plan.total(), None);
        let text: String = screen[3..3 + plan.input as usize]
            .iter()
            .map(|r| r.trim_matches(|c| c == '│' || c == ' ').to_string() + " ")
            .collect();
        let squeezed: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
        assert_eq!(squeezed, GOALS, "{screen:#?}");
        assert_eq!(app.sql_input.scroll_offsets().1, 0);
    }

    /// Under the statement, the columns of df in their type colors, narrowed to
    /// the word being typed.
    #[cfg(feature = "sql")]
    #[test]
    fn the_columns_under_sql_narrow_to_the_word_typed() {
        let mut app = sql_prompt();
        let screen = draw_sized(&mut app, 80, 8, None).join("\n");
        assert!(screen.contains("Columns"), "{screen}");
        assert!(screen.contains("Round i64"), "{screen}");
        assert!(screen.contains("Team 1 str"), "{screen}");

        app.sql_input.set_value("SELECT Te");
        let screen = draw_sized(&mut app, 80, 8, None).join("\n");
        assert!(screen.contains("Team 1 str") && screen.contains("Team 2 str"));
        assert!(!screen.contains("Round i64"), "{screen}");
    }

    /// What does not fit is counted rather than cut.
    #[cfg(feature = "sql")]
    #[test]
    fn columns_that_do_not_fit_are_counted() {
        let mut app = sql_prompt();
        app.sql_columns = (0..40)
            .map(|i| (format!("measurement_{i}"), DataType::Float64))
            .collect();
        let screen = draw_sized(&mut app, 60, 8, None).join("\n");
        let more = format!("{} ", crate::glyphs::get().ellipsis);
        assert!(
            screen.contains(&more) && screen.contains(" more"),
            "{screen}"
        );
    }

    /// An empty SQL input shows where to start.
    #[cfg(feature = "sql")]
    #[test]
    fn an_empty_sql_input_shows_an_example() {
        let mut app = sql_prompt();
        let screen = draw_sized(&mut app, 80, 8, None).join("\n");
        assert!(screen.contains(SQL_PLACEHOLDER), "{screen}");
        app.sql_input.set_value("S");
        let screen = draw_sized(&mut app, 80, 8, None).join("\n");
        assert!(!screen.contains(SQL_PLACEHOLDER), "{screen}");
    }

    /// When rows run out, the column list goes before the statement or the
    /// error lose theirs.
    #[test]
    fn the_column_list_yields_first() {
        let mut app = sql_prompt();
        app.sql_input.set_value(GOALS);
        let error = "Date: 12 of 380 values do not match the format.\nTry: a format.";
        let roomy = plan(&app, 60, 20, Some(error));
        assert_eq!(roomy.columns, COLUMN_ROWS);
        let tight = plan(&app, 60, 12, Some(error));
        assert!(tight.total() <= 12, "{tight:?}");
        assert!(tight.input >= 2 && tight.error >= 2, "{tight:?}");
    }

    #[test]
    fn words_wrap_at_spaces_and_long_ones_split() {
        assert_eq!(wrap_words("one two three", 7), ["one two", "three"]);
        assert_eq!(wrap_words("abcdefghij", 4), ["abcd", "efgh", "ij"]);
        assert_eq!(wrap_words("a\nb", 10), ["a", "b"]);
        assert_eq!(wrap_words("東京", 1), ["東", "京"]);
    }
}
