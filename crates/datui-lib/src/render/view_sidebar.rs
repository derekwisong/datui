//! The Views sidebar: the list of saved views scored against the open file,
//! the name-first save/edit form, the delete confirmation and the score
//! breakdown. Built on the `widgets::ui` kit; the rail marks focus.

use crate::render::context::RenderContext;
use crate::render::layout::centered_rect;
use crate::widgets::ui::{FormRow, FormValue, HintBar, SectionRule, Surface};
use crate::widgets::view_modal::{FormFocus, ViewModal, ViewModalMode};
use datui_cli::keys::Context;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

/// The value column's offset inside the form: past the longest label,
/// "Filename pattern:", plus two cells of air.
const LABEL_WIDTH: u16 = 19;

/// Columns the name takes in the list before the match annotation.
const NAME_WIDTH: usize = 20;

/// The match annotation's region: " same columns " is the widest.
const REASON_WIDTH: usize = 14;

/// Render the Views sidebar into the given area.
pub fn render(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut ViewModal,
    active_view_id: Option<&str>,
    ctx: &RenderContext,
) {
    match modal.mode {
        ViewModalMode::List => render_list(area, buf, modal, active_view_id, ctx),
        ViewModalMode::Create | ViewModalMode::Edit => render_form(area, buf, modal, ctx),
    }

    if modal.score_details.is_some() {
        render_score_details(area, buf, modal, ctx);
    }
}

/// The score mark: how well this view fits, relative to the best in the list.
fn score_mark(score: f64, max_score: f64, ctx: &RenderContext) -> (&'static str, Style) {
    let g = crate::glyphs::get();
    let marks = g.score_marks;
    let ratio = if max_score > 0.0 {
        score / max_score
    } else {
        0.0
    };
    let (mark, color) = if ratio >= 0.95 {
        (marks[4], ctx.success)
    } else if ratio >= 0.9 {
        (marks[3], ctx.success)
    } else if ratio >= 0.8 {
        (marks[2], ctx.success)
    } else if ratio >= 0.7 {
        (marks[2], ctx.warning)
    } else if ratio >= 0.55 {
        (marks[1], ctx.warning)
    } else if ratio >= 0.4 {
        (marks[0], ctx.warning)
    } else if ratio >= 0.2 {
        (marks[0], ctx.text_primary)
    } else {
        (marks[0], ctx.dimmed)
    };
    (mark, Style::default().fg(color))
}

/// Pad or truncate to `width` display columns, marking the cut with the
/// ellipsis glyph and never splitting a wide character.
fn fit(text: &str, width: usize) -> String {
    let fitted = crate::glyphs::fit(text, width);
    let pad = width.saturating_sub(crate::glyphs::display_width(&fitted));
    format!("{fitted}{}", " ".repeat(pad))
}

fn render_list(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut ViewModal,
    active_view_id: Option<&str>,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    // Only keys that act: with no view saved there is nothing to apply, edit,
    // delete or score.
    let footer = HintBar::from_ctx(ctx).screen(Context::Views).group("List");
    let footer = if modal.rows.is_empty() {
        footer.key("s").weight(4)
    } else {
        footer
            .key("Enter")
            .weight(5)
            .key("s")
            .weight(4)
            .key("e")
            .weight(2)
            .key("d")
            .weight(3)
            .key("i")
            .weight(1)
    };
    let footer = footer.key("Esc").weight(6);
    let content = Surface::new("Views").footer(&footer).render(area, buf, ctx);
    if content.height < 2 || content.width < 10 {
        return;
    }

    // The list's own status line, directly above the footer: a refusal is
    // said where the key was pressed, and the next key clears it.
    if let Some(status) = &modal.status {
        // Cut with a mark, never silently.
        Paragraph::new(fit(status, usize::from(content.width)))
            .style(Style::default().fg(ctx.warning))
            .render(
                Rect {
                    y: content.y + content.height - 1,
                    height: 1,
                    ..content
                },
                buf,
            );
    }

    if modal.rows.is_empty() && modal.broken_views.is_empty() {
        Paragraph::new(format!(
            "No saved views {} s saves one",
            crate::glyphs::get().middot
        ))
        .style(Style::default().fg(ctx.dimmed))
        .render(
            Rect {
                height: 1,
                ..content
            },
            buf,
        );
        return;
    }

    // Header, behind the same rail gutter the rows reserve.
    let header = format!(
        "     {:<name$} {:<reason$} {}",
        "Name",
        "Match",
        "Description",
        name = NAME_WIDTH,
        reason = REASON_WIDTH,
    );
    Paragraph::new(fit(&header, usize::from(content.width)))
        .style(Style::default().fg(ctx.text_secondary))
        .render(
            Rect {
                height: 1,
                ..content
            },
            buf,
        );

    let list_area = Rect {
        y: content.y + 1,
        height: content.height - 1,
        ..content
    };
    let max_score = modal.rows.iter().map(|row| row.score).fold(0.0, f64::max);
    let total = modal.rows.len() + modal.broken_views.len();
    let selected = modal.table_state.selected().unwrap_or(0);
    let height = list_area.height as usize;
    let offset = selected.saturating_sub(height.saturating_sub(1));
    let below = total.saturating_sub(offset + height);

    for line in 0..height.min(total.saturating_sub(offset)) {
        let i = offset + line;
        let row_area = Rect {
            y: list_area.y + line as u16,
            height: 1,
            ..list_area
        };
        let is_cursor = i == selected && i < modal.rows.len();
        if line + 1 == height && below > 0 && !is_cursor {
            Paragraph::new(format!("  {} {} more", g.ellipsis, below + 1))
                .style(Style::default().fg(ctx.dimmed))
                .render(row_area, buf);
            break;
        }

        if let Some(row) = modal.rows.get(i) {
            let is_active = active_view_id.is_some_and(|id| id == row.view.id);
            let (mark, mark_style) = score_mark(row.score, max_score, ctx);
            let check = if is_active { g.check } else { " " };
            let name_style = if is_cursor {
                Style::default().fg(ctx.accent)
            } else {
                Style::default().fg(ctx.text_primary)
            };
            let reason_span = match row.reason {
                // The annotation is a flat chip, like a section rule's count.
                Some(reason) => Span::styled(
                    format!("{:<REASON_WIDTH$}", format!(" {} ", reason.as_str())),
                    Style::default().bg(ctx.controls_bg).fg(ctx.text_primary),
                ),
                None => Span::raw(" ".repeat(REASON_WIDTH)),
            };
            let description = row
                .view
                .description
                .as_deref()
                .unwrap_or("")
                .lines()
                .next()
                .unwrap_or("")
                .to_string();
            let rail = if is_cursor { g.rail } else { " " };
            let line = Line::from(vec![
                Span::styled(rail, Style::default().fg(ctx.accent)),
                Span::styled(mark, mark_style),
                Span::raw(" "),
                Span::styled(check, Style::default().fg(ctx.accent)),
                Span::raw(" "),
                Span::styled(fit(&row.view.name, NAME_WIDTH), name_style),
                Span::raw(" "),
                reason_span,
                Span::raw(" "),
                Span::styled(description, Style::default().fg(ctx.dimmed)),
            ]);
            Paragraph::new(line)
                .style(if is_cursor {
                    ctx.highlight_style()
                } else {
                    Style::default()
                })
                .render(row_area, buf);
        } else if let Some(broken) = modal.broken_views.get(i - modal.rows.len()) {
            let line = Line::from(vec![
                Span::raw(" "),
                Span::styled(g.warning, Style::default().fg(ctx.warning)),
                Span::raw("   "),
                Span::styled(
                    fit(&broken.filename, NAME_WIDTH),
                    Style::default().fg(ctx.warning),
                ),
                Span::raw(" ".repeat(REASON_WIDTH + 2)),
                Span::styled(broken.error.clone(), Style::default().fg(ctx.dimmed)),
            ]);
            Paragraph::new(line).render(row_area, buf);
        }
    }
}

fn render_form(area: Rect, buf: &mut Buffer, modal: &mut ViewModal, ctx: &RenderContext) {
    let title = if modal.mode == ViewModalMode::Edit {
        "Edit View"
    } else {
        "Save View"
    };
    let footer = HintBar::from_ctx(ctx)
        .screen(Context::Views)
        .group("Save and edit");
    let footer = match modal.form_focus {
        // Enter types inside the multiline description; the footer names the
        // key that still saves from there on every terminal (Ctrl+Enter needs
        // the keyboard-enhancement protocol).
        FormFocus::Description => footer.key("Ctrl+J"),
        _ => footer.key("Enter"),
    }
    .weight(3);
    let footer = match modal.form_focus {
        FormFocus::Matching if modal.matching_expanded => {
            footer.key_as("Space", "Collapse").weight(2)
        }
        FormFocus::Matching => footer.key_as("Space", "Expand").weight(2),
        FormFocus::SchemaMatch => footer.key("Space").weight(2),
        _ => footer,
    };
    let footer = footer.key("Tab").weight(1).key("Esc").weight(4);
    let content = Surface::new(title).footer(&footer).render(area, buf, ctx);
    if content.height < 5 || content.width < 10 {
        return;
    }

    for (input, focus) in [
        (&mut modal.name_input, FormFocus::Name),
        (&mut modal.description_input, FormFocus::Description),
        (&mut modal.exact_path_input, FormFocus::ExactPath),
        (&mut modal.relative_path_input, FormFocus::RelativePath),
        (&mut modal.path_pattern_input, FormFocus::PathPattern),
        (
            &mut modal.filename_pattern_input,
            FormFocus::FilenamePattern,
        ),
    ] {
        input.set_focused(modal.form_focus == focus);
    }

    let row = |offset: u16, height: u16| Rect {
        y: content.y + offset,
        height: height.min((content.y + content.height).saturating_sub(content.y + offset)),
        ..content
    };

    // Name, with any validation error at the row's right edge.
    FormRow {
        label: "Name:",
        value: FormValue::Input(&modal.name_input),
        focused: modal.form_focus == FormFocus::Name,
        label_width: LABEL_WIDTH,
    }
    .render(row(0, 1), buf, ctx);
    crate::app::pointer::record_field::<ViewModal>(row(0, 1), FormFocus::Name);
    if let Some(error) = &modal.name_error {
        let width = crate::glyphs::display_width(error) as u16;
        if width < content.width {
            Paragraph::new(error.as_str())
                .style(Style::default().fg(ctx.error))
                .render(
                    Rect {
                        x: content.x + content.width - width,
                        y: content.y,
                        width,
                        height: 1,
                    },
                    buf,
                );
        }
    }

    // Description gets three rows; the input scrolls beyond them.
    FormRow {
        label: "Description:",
        value: FormValue::Input(&modal.description_input),
        focused: modal.form_focus == FormFocus::Description,
        label_width: LABEL_WIDTH,
    }
    .render(row(1, 3), buf, ctx);
    crate::app::pointer::record_field::<ViewModal>(row(1, 3), FormFocus::Description);

    // The Matching section: a rule behind the rail gutter, criteria under it
    // only while expanded. The chip counts the criteria that are set.
    let matching_focused = modal.form_focus == FormFocus::Matching;
    let rules = modal.criteria_count();
    let chip = format!("{} rule{}", rules, if rules == 1 { "" } else { "s" });
    let section_y = 5u16;
    let rail_area = Rect {
        width: 1,
        ..row(section_y, 1)
    };
    let g = crate::glyphs::get();
    Paragraph::new(if matching_focused { g.rail } else { " " })
        .style(Style::default().fg(ctx.accent))
        .render(rail_area, buf);
    let rule_area = row(section_y, 1);
    crate::app::pointer::record_field::<ViewModal>(rule_area, FormFocus::Matching);
    SectionRule {
        title: "Matching",
        chip: Some(&chip),
    }
    .render(
        Rect {
            x: rule_area.x + 1,
            width: rule_area.width.saturating_sub(1),
            ..rule_area
        },
        buf,
        ctx,
    );

    if !modal.matching_expanded {
        return;
    }

    let criteria: [(&str, FormValue, FormFocus); 5] = [
        (
            "Exact path:",
            FormValue::Input(&modal.exact_path_input),
            FormFocus::ExactPath,
        ),
        (
            "Relative path:",
            FormValue::Input(&modal.relative_path_input),
            FormFocus::RelativePath,
        ),
        (
            "Path pattern:",
            FormValue::Input(&modal.path_pattern_input),
            FormFocus::PathPattern,
        ),
        (
            "Filename pattern:",
            FormValue::Input(&modal.filename_pattern_input),
            FormFocus::FilenamePattern,
        ),
        (
            "Schema match:",
            FormValue::Toggle(modal.schema_match_enabled),
            FormFocus::SchemaMatch,
        ),
    ];
    for (i, (label, value, focus)) in criteria.into_iter().enumerate() {
        let area = row(section_y + 1 + i as u16, 1);
        if area.height == 0 {
            break;
        }
        FormRow {
            label,
            value,
            focused: modal.form_focus == focus,
            label_width: LABEL_WIDTH,
        }
        .render(area, buf, ctx);
        crate::app::pointer::record_field::<ViewModal>(area, focus);
    }

    // The table is the dataset's, not typed: echoed, never focused. The path
    // criteria fit only it.
    if let Some(table) = &modal.table {
        let area = row(section_y + 6, 1);
        if area.height > 0 {
            FormRow {
                label: "Table:",
                value: FormValue::Choice(table),
                focused: false,
                label_width: LABEL_WIDTH,
            }
            .render(area, buf, ctx);
        }
    }
}

fn render_score_details(area: Rect, buf: &mut Buffer, modal: &mut ViewModal, ctx: &RenderContext) {
    let Some((title, body)) = &modal.score_details else {
        return;
    };
    // Sized to the breakdown, not the terminal: a compact centered dialog.
    // The body, the blank above the footer, the footer and the frame.
    let height = (body.lines().count() as u16 + 4).min(area.height);
    let details_area = centered_rect(area, 56, height);
    let footer = HintBar::from_ctx(ctx)
        .screen(Context::Views)
        .group("List")
        .key("Esc");
    let content = Surface::new(title.as_str())
        .footer(&footer)
        .render(details_area, buf, ctx);
    Paragraph::new(body.as_str())
        .wrap(ratatui::widgets::Wrap { trim: false })
        .render(content, buf);
}

#[cfg(test)]
mod tests;
