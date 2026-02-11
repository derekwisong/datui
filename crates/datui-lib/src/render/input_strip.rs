//! Query / Filter / Go-to-line input strip rendering.

use crate::render::context::RenderContext;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, BorderType, Borders, Paragraph, Tabs, Widget};

/// Renders the input strip (query/fuzzy/SQL tabs, inputs, error) when in Editing mode.
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
        Some(crate::InputType::Filter) => "Filter",
        Some(crate::InputType::GoToLine) => "Go to line",
        None => "Input",
    };

    let mut border_style = Style::default();
    if has_error {
        border_style = Style::default().fg(ctx.error);
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title(title)
        .border_style(border_style);
    let inner_area = block.inner(input_area);
    block.render(input_area, buf);

    if app.input_type == Some(crate::InputType::Search) {
        let border_c = ctx.modal_border;
        let active_c = ctx.modal_border_active;
        let tab_bar_focused = app.query_focus == crate::QueryFocus::TabBar;

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(2), Constraint::Min(1)])
            .split(inner_area);

        let tab_line_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1)])
            .split(chunks[0]);
        let tab_row_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(0), Constraint::Max(40)])
            .split(tab_line_chunks[0]);
        let tab_titles = vec!["SQL-Like", "Fuzzy", "SQL"];
        let tabs = Tabs::new(tab_titles)
            .style(Style::default().fg(border_c))
            .highlight_style(
                Style::default()
                    .fg(active_c)
                    .add_modifier(Modifier::REVERSED),
            )
            .select(app.query_tab.index());
        tabs.render(tab_row_chunks[0], buf);
        let desc_text = match app.query_tab {
            crate::QueryTab::SqlLike => "select [cols] [by ...] [where ...]",
            crate::QueryTab::Fuzzy => "Search text to find matching rows",
            crate::QueryTab::Sql => {
                #[cfg(feature = "sql")]
                {
                    "Table: df"
                }
                #[cfg(not(feature = "sql"))]
                {
                    ""
                }
            }
        };
        if !desc_text.is_empty() {
            Paragraph::new(desc_text)
                .style(Style::default().fg(ctx.text_secondary))
                .alignment(Alignment::Right)
                .render(tab_row_chunks[1], buf);
        }
        let line_style = if tab_bar_focused {
            Style::default().fg(active_c)
        } else {
            Style::default().fg(border_c)
        };
        Block::default()
            .borders(Borders::BOTTOM)
            .border_type(BorderType::Rounded)
            .border_style(line_style)
            .render(tab_line_chunks[1], buf);

        match app.query_tab {
            crate::QueryTab::SqlLike => {
                if has_error {
                    let body_chunks = Layout::default()
                        .direction(Direction::Vertical)
                        .constraints([Constraint::Length(1), Constraint::Min(1)])
                        .split(chunks[1]);
                    app.query_input
                        .set_focused(app.query_focus == crate::QueryFocus::Input);
                    (&app.query_input).render(body_chunks[0], buf);
                    Paragraph::new(err_msg)
                        .style(Style::default().fg(ctx.error))
                        .wrap(ratatui::widgets::Wrap { trim: true })
                        .render(body_chunks[1], buf);
                } else {
                    app.query_input
                        .set_focused(app.query_focus == crate::QueryFocus::Input);
                    (&app.query_input).render(chunks[1], buf);
                }
            }
            crate::QueryTab::Fuzzy => {
                app.query_input.set_focused(false);
                app.sql_input.set_focused(false);
                app.fuzzy_input
                    .set_focused(app.query_focus == crate::QueryFocus::Input);
                (&app.fuzzy_input).render(chunks[1], buf);
            }
            crate::QueryTab::Sql => {
                app.query_input.set_focused(false);
                #[cfg(feature = "sql")]
                {
                    if has_error {
                        let body_chunks = Layout::default()
                            .direction(Direction::Vertical)
                            .constraints([Constraint::Length(1), Constraint::Min(1)])
                            .split(chunks[1]);
                        app.sql_input
                            .set_focused(app.query_focus == crate::QueryFocus::Input);
                        (&app.sql_input).render(body_chunks[0], buf);
                        Paragraph::new(err_msg)
                            .style(Style::default().fg(ctx.error))
                            .wrap(ratatui::widgets::Wrap { trim: true })
                            .render(body_chunks[1], buf);
                    } else {
                        app.sql_input
                            .set_focused(app.query_focus == crate::QueryFocus::Input);
                        (&app.sql_input).render(chunks[1], buf);
                    }
                }
                #[cfg(not(feature = "sql"))]
                {
                    app.sql_input.set_focused(false);
                    Paragraph::new("SQL support not compiled in (build with --features sql)")
                        .style(Style::default().fg(ctx.text_secondary))
                        .render(chunks[1], buf);
                }
            }
        }
    } else if has_error {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Min(1),
            ])
            .split(inner_area);

        (&app.query_input).render(chunks[0], buf);
        Paragraph::new(err_msg)
            .style(Style::default().fg(ctx.error))
            .wrap(ratatui::widgets::Wrap { trim: true })
            .render(chunks[2], buf);
    } else {
        (&app.query_input).render(inner_area, buf);
    }
}
