//! The Views sidebar: the list of saved views scored against the open file,
//! the name-first save/edit form, the delete confirmation and the score
//! breakdown. Built on the `widgets::ui` kit; the rail marks focus.

use crate::render::context::RenderContext;
use crate::render::layout::centered_rect_fixed;
use crate::widgets::template_modal::{FormFocus, TemplateModal, TemplateModalMode};
use crate::widgets::ui::{FormRow, FormValue, HintBar, SectionRule, Surface};
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
    modal: &mut TemplateModal,
    active_template_id: Option<&str>,
    ctx: &RenderContext,
) {
    match modal.mode {
        TemplateModalMode::List => render_list(area, buf, modal, active_template_id, ctx),
        TemplateModalMode::Create | TemplateModalMode::Edit => render_form(area, buf, modal, ctx),
    }

    if modal.delete_confirm {
        render_delete_confirm(area, buf, modal, ctx);
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
    let g = crate::glyphs::get();
    let count = crate::glyphs::display_width(text);
    if count <= width {
        let pad = width - count;
        format!("{text}{}", " ".repeat(pad))
    } else {
        let ellipsis_width = crate::glyphs::display_width(g.ellipsis);
        let cut = crate::glyphs::take_columns(text, width.saturating_sub(ellipsis_width));
        format!("{cut}{}", g.ellipsis)
    }
}

fn render_list(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut TemplateModal,
    active_template_id: Option<&str>,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    let footer = HintBar::from_ctx(ctx)
        .hint_weighted("Enter", "Apply", 5)
        .hint_weighted("s", "Save", 4)
        .hint_weighted("e", "Edit", 2)
        .hint_weighted("d", "Delete", 3)
        .hint_weighted("i", "Score", 1)
        .hint_weighted("Esc", "Close", 6);
    let content = Surface::new("Views").footer(&footer).render(area, buf, ctx);
    if content.height < 2 || content.width < 10 {
        return;
    }

    // The list's own status line, directly above the footer: a refusal is
    // said where the key was pressed, and the next key clears it.
    if let Some(status) = &modal.status {
        Paragraph::new(status.as_str())
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

    if modal.rows.is_empty() && modal.broken_templates.is_empty() {
        Paragraph::new("No saved views. s saves what the table shows now.")
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
    Paragraph::new(header)
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
    let total = modal.rows.len() + modal.broken_templates.len();
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
            let is_active = active_template_id.is_some_and(|id| id == row.template.id);
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
                .template
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
                Span::styled(fit(&row.template.name, NAME_WIDTH), name_style),
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
        } else if let Some(broken) = modal.broken_templates.get(i - modal.rows.len()) {
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

fn render_form(area: Rect, buf: &mut Buffer, modal: &mut TemplateModal, ctx: &RenderContext) {
    let title = if modal.mode == TemplateModalMode::Edit {
        "Edit View"
    } else {
        "Save View"
    };
    let mut footer = HintBar::from_ctx(ctx);
    footer = match modal.form_focus {
        // Enter types inside the multiline description; the footer names the
        // key that still saves from there on every terminal (Ctrl+Enter needs
        // the keyboard-enhancement protocol).
        FormFocus::Description => footer.hint_weighted("^J", "Save", 3),
        _ => footer.hint_weighted("Enter", "Save", 3),
    };
    footer = match modal.form_focus {
        FormFocus::Matching if modal.matching_expanded => {
            footer.hint_weighted("Space", "Collapse", 2)
        }
        FormFocus::Matching => footer.hint_weighted("Space", "Expand", 2),
        FormFocus::SchemaMatch => footer.hint_weighted("Space", "Toggle", 2),
        _ => footer,
    };
    let footer = footer
        .hint_weighted("Tab", "Next", 1)
        .hint_weighted("Esc", "Cancel", 4);
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
    if let Some(error) = &modal.name_error {
        let width = error.chars().count() as u16;
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
    SectionRule {
        title: "Matching",
        chip: Some(&chip),
        focused: matching_focused,
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
    }
}

fn render_delete_confirm(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut TemplateModal,
    ctx: &RenderContext,
) {
    let Some(template) = modal.selected_template() else {
        return;
    };
    let message = format!("Delete \"{}\"? This cannot be undone.", template.name);
    const WIDTH: u16 = 52;
    const HEIGHT: u16 = 6;
    let confirm_area = centered_rect_fixed(area, WIDTH, HEIGHT);
    let footer = HintBar::from_ctx(ctx)
        .hint_weighted("Enter", "Delete", 1)
        .hint_weighted("Esc", "Cancel", 2);
    let content = Surface::new("Delete View")
        .footer(&footer)
        .render(confirm_area, buf, ctx);
    Paragraph::new(message)
        .wrap(ratatui::widgets::Wrap { trim: false })
        .render(content, buf);
}

fn render_score_details(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut TemplateModal,
    ctx: &RenderContext,
) {
    let Some((title, body)) = &modal.score_details else {
        return;
    };
    // Sized to the breakdown, not the terminal: a compact centered dialog.
    // The body, the blank above the footer, the footer and the frame.
    let height = (body.lines().count() as u16 + 4).min(area.height);
    let details_area = centered_rect_fixed(area, 56, height);
    let footer = HintBar::from_ctx(ctx).hint("Esc", "Close");
    let content = Surface::new(title.as_str())
        .footer(&footer)
        .render(details_area, buf, ctx);
    Paragraph::new(body.as_str())
        .wrap(ratatui::widgets::Wrap { trim: false })
        .render(content, buf);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::template::{MatchCriteria, MatchReason, Template, TemplateSettings};
    use crate::widgets::template_modal::ViewRow;
    use std::time::SystemTime;

    fn a_template(name: &str, description: Option<&str>) -> Template {
        Template {
            id: name.to_string(),
            name: name.to_string(),
            description: description.map(str::to_string),
            created: SystemTime::now(),
            last_used: None,
            usage_count: 0,
            last_matched_file: None,
            match_criteria: MatchCriteria {
                exact_path: None,
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
            },
            settings: TemplateSettings {
                query: None,
                sql_query: None,
                fuzzy_query: None,
                filters: Vec::new(),
                sort_columns: Vec::new(),
                sort_descending: Vec::new(),
                sort_ascending: true,
                column_order: Vec::new(),
                locked_columns_count: 0,
                pivot: None,
                melt: None,
                reshape_source: None,
            },
        }
    }

    fn list_modal() -> TemplateModal {
        let mut modal = TemplateModal::new();
        modal.active = true;
        modal.rows = vec![
            ViewRow {
                template: a_template("salary review", Some("Sorted by salary")),
                score: 2000.0,
                reason: Some(MatchReason::SameFile),
            },
            ViewRow {
                template: a_template("quarterly report", None),
                score: 40.0,
                reason: None,
            },
        ];
        modal.table_state.select(Some(0));
        modal
    }

    fn render_to_rows(modal: &mut TemplateModal, width: u16, height: u16) -> Vec<String> {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        render(area, &mut buf, modal, Some("salary review"), &ctx);
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect()
    }

    /// One border, the rows inside it, the reason annotation on the row whose
    /// criteria fit, and the footer chips on the last inner row.
    #[test]
    fn the_list_is_one_surface_with_annotated_rows() {
        let mut modal = list_modal();
        let rows = render_to_rows(&mut modal, 80, 16);
        assert!(
            rows[0].contains("Views"),
            "title on the frame: {:?}",
            rows[0]
        );
        for row in &rows[1..15] {
            assert!(
                !row.contains('╭') && !row.contains('╰'),
                "a second border inside the surface: {row:?}"
            );
        }
        let g = crate::glyphs::get();
        let cursor_row = rows
            .iter()
            .find(|r| r.contains("salary review"))
            .expect("the view is listed");
        // Past the frame and its gutter, the row starts with the rail.
        assert_eq!(
            cursor_row.chars().nth(2).map(String::from).as_deref(),
            Some(g.rail),
            "the cursor row carries the rail: {cursor_row:?}"
        );
        assert!(
            cursor_row.contains("same file"),
            "the matching row says why: {cursor_row:?}"
        );
        assert!(cursor_row.contains(g.check), "the active view is checked");
        let other_row = rows
            .iter()
            .find(|r| r.contains("quarterly report"))
            .expect("the second view is listed");
        assert!(
            !other_row.contains("same file")
                && !other_row.contains("same columns")
                && !other_row.contains("glob"),
            "a view that fits nothing carries no annotation: {other_row:?}"
        );
        let footer = &rows[14];
        for chip in ["Apply", "Save", "Edit", "Delete", "Score", "Close"] {
            assert!(footer.contains(chip), "footer misses {chip}: {footer:?}");
        }
    }

    /// The form is name-first with the criteria folded away; expanding walks
    /// them in, collapsed they are absent.
    #[test]
    fn the_form_collapses_the_matching_section() {
        let mut modal = list_modal();
        modal.mode = TemplateModalMode::Create;
        modal.name_input.set_value("salaries");
        modal.schema_match_enabled = true;

        let rows = render_to_rows(&mut modal, 80, 20);
        assert!(rows[0].contains("Save View"), "got {:?}", rows[0]);
        let text = rows.join("\n");
        assert!(text.contains("Name:"));
        assert!(text.contains("Matching"));
        assert!(text.contains("1 rule"), "the chip counts the set criteria");
        assert!(
            !text.contains("Exact path:"),
            "collapsed criteria stay hidden"
        );

        modal.matching_expanded = true;
        let text = render_to_rows(&mut modal, 80, 20).join("\n");
        for label in [
            "Exact path:",
            "Relative path:",
            "Path pattern:",
            "Filename pattern:",
            "Schema match:",
        ] {
            assert!(text.contains(label), "expanded form misses {label}");
        }
    }

    /// The delete confirmation is a small Surface with its keys in the
    /// footer — no bordered buttons.
    #[test]
    fn delete_confirm_is_chips_not_buttons() {
        let mut modal = list_modal();
        modal.delete_confirm = true;
        let text = render_to_rows(&mut modal, 80, 20).join("\n");
        assert!(text.contains("Delete View"));
        assert!(text.contains("salary review"));
        assert!(text.contains("Cancel") && text.contains("Delete"));
    }

    #[test]
    fn a_tiny_area_never_panics() {
        for (w, h) in [(0, 0), (5, 3), (12, 4), (30, 6)] {
            let mut modal = list_modal();
            render_to_rows(&mut modal, w, h);
            modal.mode = TemplateModalMode::Create;
            modal.matching_expanded = true;
            render_to_rows(&mut modal, w, h);
        }
    }
}
