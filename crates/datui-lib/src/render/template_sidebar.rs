//! Template sidebar rendering (list/create/edit, delete confirm, score details).

use crate::render::context::RenderContext;
use crate::render::layout::{centered_rect, centered_rect_fixed};
use crate::widgets::template_modal::{CreateFocus, TemplateFocus, TemplateModalMode};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, Paragraph, Row, StatefulWidget, Table, Widget,
};

/// Renders the template sidebar when template_modal is active.
pub fn render(
    sort_area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &mut crate::App,
    ctx: &RenderContext,
) {
    Clear.render(sort_area, buf);
    let modal_title = match app.template_modal.mode {
        TemplateModalMode::List => "Templates",
        TemplateModalMode::Create => "Create Template",
        TemplateModalMode::Edit => "Edit Template",
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title(modal_title);
    let inner_area = block.inner(sort_area);
    block.render(sort_area, buf);

    match app.template_modal.mode {
        TemplateModalMode::List => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(0), Constraint::Length(1)])
                .split(inner_area);

            let max_score = app
                .template_modal
                .templates
                .iter()
                .map(|(_, score)| *score)
                .fold(0.0, f64::max);

            let score_col_width = 2;
            let active_col_width = 1;
            let name_col_width = 20;

            let rows: Vec<Row> = app
                .template_modal
                .templates
                .iter()
                .map(|(template, score)| {
                    let is_active = app
                        .active_template_id
                        .as_ref()
                        .map(|id| id == &template.id)
                        .unwrap_or(false);

                    let score_ratio = if max_score > 0.0 {
                        score / max_score
                    } else {
                        0.0
                    };
                    let (circle_char, circle_color) = if score_ratio >= 0.8 {
                        if score_ratio >= 0.95 {
                            ('●', ctx.success)
                        } else if score_ratio >= 0.9 {
                            ('◉', ctx.success)
                        } else {
                            ('◐', ctx.success)
                        }
                    } else if score_ratio >= 0.4 {
                        if score_ratio >= 0.7 {
                            ('◐', ctx.warning)
                        } else if score_ratio >= 0.55 {
                            ('◑', ctx.warning)
                        } else {
                            ('○', ctx.warning)
                        }
                    } else if score_ratio >= 0.2 {
                        ('○', ctx.text_primary)
                    } else {
                        ('○', ctx.dimmed)
                    };

                    let score_cell = Cell::from(circle_char.to_string())
                        .style(Style::default().fg(circle_color));

                    let active_cell = if is_active {
                        Cell::from("✓")
                    } else {
                        Cell::from(" ")
                    };

                    let name_cell = Cell::from(template.name.clone());

                    let desc = template.description.as_deref().unwrap_or("");
                    let first_line = desc.lines().next().unwrap_or("");
                    let desc_display = first_line.to_string();
                    let desc_cell = Cell::from(desc_display);

                    Row::new(vec![score_cell, active_cell, name_cell, desc_cell])
                })
                .collect();

            let header = Row::new(vec![
                Cell::from("●").style(Style::default()),
                Cell::from(" ").style(Style::default()),
                Cell::from("Name").style(Style::default()),
                Cell::from("Description").style(Style::default()),
            ])
            .style(Style::default().add_modifier(Modifier::UNDERLINED));

            let table_border_style = if app.template_modal.focus == TemplateFocus::TemplateList {
                Style::default().fg(ctx.modal_border_active)
            } else {
                Style::default()
            };

            let table = Table::new(
                rows,
                [
                    Constraint::Length(score_col_width),
                    Constraint::Length(active_col_width),
                    Constraint::Length(name_col_width),
                    Constraint::Min(0),
                ],
            )
            .header(header)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded)
                    .title("Templates")
                    .border_style(table_border_style),
            )
            .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

            StatefulWidget::render(table, chunks[0], buf, &mut app.template_modal.table_state);

            let mut hint_line = Line::default();
            hint_line.spans.push(Span::styled(
                "Enter",
                Style::default()
                    .fg(ctx.keybind_hints)
                    .add_modifier(Modifier::BOLD),
            ));
            hint_line.spans.push(Span::raw(" Apply "));
            hint_line.spans.push(Span::styled(
                "s",
                Style::default()
                    .fg(ctx.keybind_hints)
                    .add_modifier(Modifier::BOLD),
            ));
            hint_line.spans.push(Span::raw(" Create "));
            hint_line.spans.push(Span::styled(
                "e",
                Style::default()
                    .fg(ctx.keybind_hints)
                    .add_modifier(Modifier::BOLD),
            ));
            hint_line.spans.push(Span::raw(" Edit "));
            hint_line.spans.push(Span::styled(
                "d",
                Style::default()
                    .fg(ctx.keybind_hints)
                    .add_modifier(Modifier::BOLD),
            ));
            hint_line.spans.push(Span::raw(" Delete "));
            hint_line.spans.push(Span::styled(
                "Esc",
                Style::default()
                    .fg(ctx.keybind_hints)
                    .add_modifier(Modifier::BOLD),
            ));
            hint_line.spans.push(Span::raw(" Close"));

            Paragraph::new(vec![hint_line]).render(chunks[1], buf);
        }
        TemplateModalMode::Create | TemplateModalMode::Edit => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Length(6),
                    Constraint::Length(3),
                    Constraint::Length(3),
                    Constraint::Length(3),
                    Constraint::Length(3),
                    Constraint::Length(3),
                    Constraint::Length(3),
                ])
                .split(inner_area);

            let name_style = if app.template_modal.create_focus == CreateFocus::Name {
                Style::default().fg(ctx.modal_border_active)
            } else {
                Style::default()
            };
            let name_title = if let Some(error) = &app.template_modal.name_error {
                format!("Name {}", error)
            } else {
                "Name".to_string()
            };
            let name_block = Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .title(name_title)
                .title_style(if app.template_modal.name_error.is_some() {
                    Style::default().fg(ctx.error)
                } else {
                    Style::default().add_modifier(Modifier::BOLD)
                })
                .border_style(name_style);
            let name_inner = name_block.inner(chunks[0]);
            name_block.render(chunks[0], buf);
            let is_focused = app.template_modal.create_focus == CreateFocus::Name;
            app.template_modal.create_name_input.set_focused(is_focused);
            (&app.template_modal.create_name_input).render(name_inner, buf);

            let desc_style = if app.template_modal.create_focus == CreateFocus::Description {
                Style::default().fg(ctx.modal_border_active)
            } else {
                Style::default()
            };
            let desc_block = Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .title("Description")
                .border_style(desc_style);
            let desc_inner = desc_block.inner(chunks[1]);
            desc_block.render(chunks[1], buf);
            let is_focused = app.template_modal.create_focus == CreateFocus::Description;
            app.template_modal
                .create_description_input
                .set_focused(is_focused);
            app.template_modal
                .create_description_input
                .ensure_cursor_visible(desc_inner.height, desc_inner.width);
            (&app.template_modal.create_description_input).render(desc_inner, buf);

            for (chunk_idx, (focus, title)) in [
                (CreateFocus::ExactPath, "Exact Path"),
                (CreateFocus::RelativePath, "Relative Path"),
                (CreateFocus::PathPattern, "Path Pattern"),
                (CreateFocus::FilenamePattern, "Filename Pattern"),
            ]
            .iter()
            .enumerate()
            {
                let style = if app.template_modal.create_focus == *focus {
                    Style::default().fg(ctx.modal_border_active)
                } else {
                    Style::default()
                };
                let block = Block::default()
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded)
                    .title(*title)
                    .border_style(style);
                let inner = block.inner(chunks[2 + chunk_idx]);
                block.render(chunks[2 + chunk_idx], buf);
                let is_focused = app.template_modal.create_focus == *focus;
                match focus {
                    CreateFocus::ExactPath => {
                        app.template_modal
                            .create_exact_path_input
                            .set_focused(is_focused);
                        (&app.template_modal.create_exact_path_input).render(inner, buf);
                    }
                    CreateFocus::RelativePath => {
                        app.template_modal
                            .create_relative_path_input
                            .set_focused(is_focused);
                        (&app.template_modal.create_relative_path_input).render(inner, buf);
                    }
                    CreateFocus::PathPattern => {
                        app.template_modal
                            .create_path_pattern_input
                            .set_focused(is_focused);
                        (&app.template_modal.create_path_pattern_input).render(inner, buf);
                    }
                    CreateFocus::FilenamePattern => {
                        app.template_modal
                            .create_filename_pattern_input
                            .set_focused(is_focused);
                        (&app.template_modal.create_filename_pattern_input).render(inner, buf);
                    }
                    _ => {}
                }
            }

            let schema_style = if app.template_modal.create_focus == CreateFocus::SchemaMatch {
                Style::default().fg(ctx.modal_border_active)
            } else {
                Style::default()
            };
            let schema_text = if app.template_modal.create_schema_match_enabled {
                "Enabled"
            } else {
                "Disabled"
            };
            Paragraph::new(schema_text)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .title("Schema Match")
                        .border_style(schema_style),
                )
                .render(chunks[6], buf);

            let btn_layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(chunks[7]);

            let save_style = if app.template_modal.create_focus == CreateFocus::SaveButton {
                Style::default().fg(ctx.modal_border_active)
            } else {
                Style::default()
            };
            Paragraph::new("Save")
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .border_style(save_style),
                )
                .centered()
                .render(btn_layout[0], buf);

            let cancel_create_style =
                if app.template_modal.create_focus == CreateFocus::CancelButton {
                    Style::default().fg(ctx.modal_border_active)
                } else {
                    Style::default()
                };
            Paragraph::new("Cancel")
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .border_style(cancel_create_style),
                )
                .centered()
                .render(btn_layout[1], buf);
        }
    }

    if app.template_modal.delete_confirm {
        if let Some(template) = app.template_modal.selected_template() {
            // Fixed size so the modal does not shrink with window height (message 3 lines + buttons 3 lines + title/border)
            const DELETE_CONFIRM_WIDTH: u16 = 52;
            const DELETE_CONFIRM_HEIGHT: u16 = 10;
            let confirm_area =
                centered_rect_fixed(sort_area, DELETE_CONFIRM_WIDTH, DELETE_CONFIRM_HEIGHT);
            Clear.render(confirm_area, buf);
            let block = Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .title("Delete Template");
            let inner_area = block.inner(confirm_area);
            block.render(confirm_area, buf);

            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(0), Constraint::Length(3)])
                .split(inner_area);

            let message = format!(
                "Are you sure you want to delete the template \"{}\"?\n\nThis action cannot be undone.",
                template.name
            );
            Paragraph::new(message)
                .wrap(ratatui::widgets::Wrap { trim: false })
                .render(chunks[0], buf);

            let btn_layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(chunks[1]);

            let mut delete_line = Line::default();
            delete_line.spans.push(Span::styled(
                "D",
                Style::default()
                    .fg(ctx.keybind_hints)
                    .add_modifier(Modifier::BOLD),
            ));
            delete_line.spans.push(Span::raw("elete"));

            let delete_style = if app.template_modal.delete_confirm_focus {
                Style::default().fg(ctx.modal_border_active)
            } else {
                Style::default()
            };
            Paragraph::new(vec![delete_line])
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .border_style(delete_style),
                )
                .centered()
                .render(btn_layout[0], buf);

            let cancel_style = if !app.template_modal.delete_confirm_focus {
                Style::default().fg(ctx.modal_border_active)
            } else {
                Style::default()
            };
            Paragraph::new("Cancel")
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .border_style(cancel_style),
                )
                .centered()
                .render(btn_layout[1], buf);
        }
    }

    if app.template_modal.show_score_details {
        if let Some((template, score)) = app
            .template_modal
            .table_state
            .selected()
            .and_then(|idx| app.template_modal.templates.get(idx))
        {
            if let Some(ref state) = app.data_table_state {
                if let Some(ref path) = app.path {
                    let details_area = centered_rect(sort_area, 60, 50);
                    Clear.render(details_area, buf);
                    let block = Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .title(format!("Score Details: {}", template.name));
                    let inner_area = block.inner(details_area);
                    block.render(details_area, buf);

                    let exact_path_match = template
                        .match_criteria
                        .exact_path
                        .as_ref()
                        .map(|exact| exact == path)
                        .unwrap_or(false);

                    let relative_path_match =
                        if let Some(relative_path) = &template.match_criteria.relative_path {
                            if let Ok(cwd) = std::env::current_dir() {
                                if let Ok(rel_path) = path.strip_prefix(&cwd) {
                                    rel_path.to_string_lossy() == *relative_path
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        };

                    let exact_schema_match =
                        if let Some(required_cols) = &template.match_criteria.schema_columns {
                            let file_cols: std::collections::HashSet<&str> =
                                state.schema.iter_names().map(|s| s.as_str()).collect();
                            let required_cols_set: std::collections::HashSet<&str> =
                                required_cols.iter().map(|s| s.as_str()).collect();
                            required_cols_set.is_subset(&file_cols)
                                && file_cols.len() == required_cols_set.len()
                        } else {
                            false
                        };

                    let mut details = format!("Total Score: {:.1}\n\n", score);

                    if exact_path_match && exact_schema_match {
                        details.push_str("Exact Path + Exact Schema: 2000.0\n");
                    } else if exact_path_match {
                        details.push_str("Exact Path: 1000.0\n");
                    } else if relative_path_match && exact_schema_match {
                        details.push_str("Relative Path + Exact Schema: 1950.0\n");
                    } else if relative_path_match {
                        details.push_str("Relative Path: 950.0\n");
                    } else if exact_schema_match {
                        details.push_str("Exact Schema: 900.0\n");
                    } else {
                        if let Some(pattern) = &template.match_criteria.path_pattern {
                            if path
                                .to_str()
                                .map(|p| p.contains(pattern.trim_end_matches("/*")))
                                .unwrap_or(false)
                            {
                                details.push_str("Path Pattern Match: 50.0+\n");
                            }
                        }
                        if let Some(pattern) = &template.match_criteria.filename_pattern {
                            if path
                                .file_name()
                                .and_then(|f| f.to_str())
                                .map(|f| {
                                    f.contains(pattern.trim_end_matches("*")) || pattern == "*"
                                })
                                .unwrap_or(false)
                            {
                                details.push_str("Filename Pattern Match: 30.0+\n");
                            }
                        }
                        if let Some(required_cols) = &template.match_criteria.schema_columns {
                            let file_cols: std::collections::HashSet<&str> =
                                state.schema.iter_names().map(|s| s.as_str()).collect();
                            let matching_count = required_cols
                                .iter()
                                .filter(|col| file_cols.contains(col.as_str()))
                                .count();
                            if matching_count > 0 {
                                details.push_str(&format!(
                                    "Partial Schema Match: {:.1} ({} columns)\n",
                                    matching_count as f64 * 2.0,
                                    matching_count
                                ));
                            }
                        }
                    }

                    if template.usage_count > 0 {
                        details.push_str(&format!(
                            "Usage Count: {:.1}\n",
                            (template.usage_count.min(10) as f64) * 1.0
                        ));
                    }
                    if let Some(last_used) = template.last_used {
                        if let Ok(duration) = std::time::SystemTime::now().duration_since(last_used)
                        {
                            let days_since = duration.as_secs() / 86400;
                            if days_since <= 7 {
                                details.push_str("Recent Usage: 5.0\n");
                            } else if days_since <= 30 {
                                details.push_str("Recent Usage: 2.0\n");
                            }
                        }
                    }
                    if let Ok(duration) =
                        std::time::SystemTime::now().duration_since(template.created)
                    {
                        let months_old = (duration.as_secs() / (30 * 86400)) as f64;
                        if months_old > 0.0 {
                            details.push_str(&format!("Age Penalty: -{:.1}\n", months_old * 1.0));
                        }
                    }

                    Paragraph::new(details)
                        .wrap(ratatui::widgets::Wrap { trim: false })
                        .render(inner_area, buf);
                }
            }
        }
    }
}
