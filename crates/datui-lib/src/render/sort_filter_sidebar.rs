//! Sort & Filter sidebar: a Columns tab (every per-column property in one
//! flat list) and a Filters tab (one row per statement, edited inline through
//! Pickers). Built on the `widgets::ui` kit; the rail marks focus.

use crate::filter_modal::{FilterEditStep, FilterModal};
use crate::render::context::RenderContext;
use crate::sort_filter_modal::{SortFilterFocus, SortFilterModal, SortFilterTab};
use crate::sort_modal::SortFocus;
use crate::widgets::ui::{HintBar, Picker, Surface};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

// (the inline pickers size themselves to the room below the edit row)

/// Render the Sort & Filter sidebar into the given area.
pub fn render(area: Rect, buf: &mut Buffer, modal: &mut SortFilterModal, ctx: &RenderContext) {
    let sort_tab = modal.active_tab == SortFilterTab::Sort;
    let editing = modal.filter.editor.is_some();

    // The footer names what Enter does right now: apply, or — while the
    // Filters list or its editor owns Enter — the apply key that still works.
    // That is Ctrl+J, not Ctrl+Enter: without the keyboard-enhancement
    // protocol many terminals send Ctrl+Enter as a plain Enter.
    let footer = if editing {
        HintBar::from_ctx(ctx).hint_weighted("^J", "Apply", 2)
    } else if sort_tab {
        HintBar::from_ctx(ctx)
            .hint_weighted("Enter", "Apply", 3)
            .hint_weighted("Tab", "Next", 1)
            .hint_weighted("Esc", "Cancel", 4)
    } else {
        HintBar::from_ctx(ctx)
            .hint_weighted("a", "Apply", 3)
            .hint_weighted("Tab", "Next", 1)
            .hint_weighted("Esc", "Cancel", 4)
    };
    let footer = if !editing && modal.sort.has_unapplied_changes {
        // Staged edits give the apply chip a quiet accent: something is waiting.
        footer.accent(if sort_tab { "Enter" } else { "a" })
    } else {
        footer
    };
    let content = Surface::new("Sort & Filter")
        .footer(&footer)
        .render(area, buf, ctx);
    if content.height < 3 || content.width < 10 {
        return;
    }

    // Tab line: the active tab carries the accent; the rail says the tab bar
    // itself holds focus.
    let g = crate::glyphs::get();
    let on_tab_bar = modal.focus == SortFilterFocus::TabBar;
    // The rail sits beside the active tab's name, so it never reads as
    // marking a tab the surface is not on. The slot is reserved either way,
    // so focus arriving or leaving moves nothing.
    let mark = |active: bool| {
        if on_tab_bar && active { g.rail } else { " " }
    };
    let tab_style = |active: bool| {
        if active {
            Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(ctx.text_secondary)
        }
    };
    let tab_line = Line::from(vec![
        Span::styled(mark(sort_tab), Style::default().fg(ctx.accent)),
        Span::styled("Columns", tab_style(sort_tab)),
        Span::styled(format!(" {}", g.rule), Style::default().fg(ctx.dimmed)),
        Span::styled(mark(!sort_tab), Style::default().fg(ctx.accent)),
        Span::styled("Filters", tab_style(!sort_tab)),
    ]);
    Paragraph::new(tab_line).render(
        Rect {
            height: 1,
            ..content
        },
        buf,
    );

    // One context line of keys sits directly above the Surface footer.
    let hints_area = Rect {
        y: content.y + content.height - 1,
        height: 1,
        ..content
    };
    let body = Rect {
        y: content.y + 1,
        height: content.height.saturating_sub(2),
        ..content
    };
    let hints = if sort_tab {
        render_columns_tab(body, buf, modal, ctx);
        // Why the last key did nothing takes the hint line until the next key,
        // so arriving and leaving move nothing.
        if let Some(status) = &modal.sort.status {
            Paragraph::new(status.as_str())
                .style(Style::default().fg(ctx.warning))
                .render(hints_area, buf);
            return;
        }
        HintBar::from_ctx(ctx)
            .hint_weighted("Space", "Sort", 5)
            .hint_weighted("1-9", "Jump", 4)
            .hint_weighted("L", "Lock", 3)
            .hint_weighted("v", "Hide", 2)
            .hint_weighted("C", "Clear", 1)
    } else {
        let filters_focused = modal.focus == SortFilterFocus::Body;
        render_filters_tab(body, buf, &mut modal.filter, filters_focused, ctx);
        match modal.filter.editor.as_ref().map(|editor| editor.step) {
            None => HintBar::from_ctx(ctx)
                .hint_weighted("Enter", "Add/Edit", 5)
                .hint_weighted("Space", "And/Or", 3)
                .hint_weighted("d", "Delete", 4)
                .hint_weighted("C", "Clear", 2),
            Some(FilterEditStep::Value) => HintBar::from_ctx(ctx)
                .hint_weighted("Enter", "Save", 2)
                .hint_weighted("Esc", "Back", 1),
            Some(_) => HintBar::from_ctx(ctx)
                .hint_weighted("type", "Narrow", 1)
                .hint_weighted("Enter", "Next", 3)
                .hint_weighted("Esc", "Back", 2),
        }
    };
    hints.render_flush(hints_area, buf);
}

/// The Columns tab: find row, header, one row per column.
fn render_columns_tab(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut SortFilterModal,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    let on_body = modal.focus == SortFilterFocus::Body;
    let on_find = on_body && modal.sort.focus == SortFocus::Filter;
    let on_list = on_body && modal.sort.focus == SortFocus::ColumnList;

    // find row
    let find_rail = if on_find { g.rail } else { " " };
    let label_style = if on_find {
        Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(ctx.label)
    };
    Paragraph::new(Line::from(vec![
        Span::styled(find_rail, Style::default().fg(ctx.accent)),
        Span::styled("find: ", label_style),
    ]))
    .render(Rect { height: 1, ..area }, buf);
    let input_area = Rect {
        x: area.x + 7,
        width: area.width.saturating_sub(7),
        height: 1,
        ..area
    };
    modal.sort.filter_input.set_focused(on_find);
    (&modal.sort.filter_input).render(input_area, buf);

    if area.height < 3 {
        return;
    }
    // header
    // One leading gutter column, as the rows below reserve for the rail.
    let header = format!("  {:<5}{:<6}{}", "Lock", "Sort", "Column");
    Paragraph::new(header)
        .style(Style::default().fg(ctx.text_secondary))
        .render(
            Rect {
                y: area.y + 1,
                height: 1,
                ..area
            },
            buf,
        );

    // rows, scrolled to keep the cursor in view
    let list_area = Rect {
        y: area.y + 2,
        height: area.height - 2,
        ..area
    };
    let filtered = modal.sort.filtered_columns();
    let selected = modal.sort.table_state.selected().unwrap_or(0);
    let height = list_area.height as usize;
    let offset = selected.saturating_sub(height.saturating_sub(1));
    let below = filtered.len().saturating_sub(offset + height);
    for row in 0..height.min(filtered.len().saturating_sub(offset)) {
        let i = offset + row;
        let row_area = Rect {
            y: list_area.y + row as u16,
            height: 1,
            ..list_area
        };
        let is_cursor = i == selected;
        if row + 1 == height && below > 0 && !is_cursor {
            Paragraph::new(format!("  {} {} more", g.ellipsis, below + 1))
                .style(Style::default().fg(ctx.dimmed))
                .render(row_area, buf);
            break;
        }
        let (_, column) = &filtered[i];
        let lock = if column.is_locked {
            g.dot_full
        } else if column.is_to_be_locked {
            g.dot_half
        } else {
            " "
        };
        let sort = match column.sort_order {
            Some(order) => format!(
                "{:>2}{}",
                order,
                if column.sort_descending {
                    g.sort_desc
                } else {
                    g.sort_asc
                }
            ),
            None => "   ".to_string(),
        };
        let hidden = if column.is_visible {
            String::new()
        } else {
            format!(" {}", g.hidden_mark)
        };
        let rail = if is_cursor && on_list { g.rail } else { " " };
        let mut style = if !column.is_visible || column.is_to_be_locked {
            Style::default().fg(ctx.dimmed)
        } else if is_cursor {
            Style::default().fg(ctx.accent)
        } else {
            Style::default().fg(ctx.text_primary)
        };
        if is_cursor && on_list {
            style = style.patch(ctx.highlight_style());
        }
        let text = format!(" {:<5}{:<6}{}{}", lock, sort, column.name, hidden);
        Paragraph::new(Line::from(vec![
            Span::styled(rail, Style::default().fg(ctx.accent)),
            Span::styled(text, style),
        ]))
        .style(if is_cursor && on_list {
            ctx.highlight_style()
        } else {
            Style::default()
        })
        .render(row_area, buf);
    }
}

/// The Filters tab: one row per statement, the add row, and the inline editor.
fn render_filters_tab(
    area: Rect,
    buf: &mut Buffer,
    filter: &mut FilterModal,
    focused: bool,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    // Column widths shared by every row, so the three parts line up.
    let col_w = filter
        .statements
        .iter()
        .map(|s| s.column.chars().count())
        .max()
        .unwrap_or(6)
        .clamp(6, 14);
    let op_w = 9; // "!contains"

    let mut y = area.y;
    let bottom = area.y + area.height;
    for row in 0..filter.row_count() {
        if y >= bottom {
            break;
        }
        let row_area = Rect {
            y,
            height: 1,
            ..area
        };
        y += 1;
        let is_cursor = row == filter.cursor;
        let under_edit = filter
            .editor
            .as_ref()
            .is_some_and(|editor| editor.editing == Some(row))
            || (filter.editor.as_ref().is_some_and(|e| e.editing.is_none())
                && row == filter.statements.len());

        if under_edit {
            let rows_owed = (filter.row_count() - row - 1) as u16;
            let editor = filter.editor.as_mut().expect("checked above");
            // The row under edit: the three steps on one line, the active one
            // accented; the picker for the active step drops in below.
            let step = editor.step;
            let seg_style = |active: bool| {
                if active {
                    Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(ctx.text_primary)
                }
            };
            let column_text = match editor.column.selected_original() {
                Some(i) if step != FilterEditStep::Column => filter.available_columns[i].clone(),
                _ => format!("{}{}", editor.column.filter, g.cursor),
            };
            let operator_text = if step == FilterEditStep::Operator {
                format!("{}{}", editor.operator.filter, g.cursor)
            } else {
                editor
                    .operator
                    .selected_original()
                    .and_then(|i| crate::filter_modal::FilterOperator::iterator().nth(i))
                    .map(|op| op.as_str().to_string())
                    .unwrap_or_default()
            };
            let mut spans = vec![
                Span::styled(g.rail, Style::default().fg(ctx.accent)),
                Span::styled(
                    format!("{:<w$} ", column_text, w = col_w),
                    seg_style(step == FilterEditStep::Column),
                ),
                Span::styled(
                    format!("{:<w$} ", operator_text, w = op_w),
                    seg_style(step == FilterEditStep::Operator),
                ),
            ];
            if step == FilterEditStep::Value {
                spans.push(Span::styled(
                    editor.value.value().to_string(),
                    seg_style(true),
                ));
                spans.push(Span::styled(g.cursor, Style::default().fg(ctx.accent)));
            } else {
                spans.push(Span::styled(
                    editor.value.value().to_string(),
                    Style::default().fg(ctx.dimmed),
                ));
            }
            Paragraph::new(Line::from(spans)).render(row_area, buf);

            // The active step's picker, narrowed as typed.
            let picker_state = match step {
                FilterEditStep::Column => Some(&editor.column),
                FilterEditStep::Operator => Some(&editor.operator),
                FilterEditStep::Value => None,
            };
            if let Some(state) = picker_state {
                // All the room down to the rows still owed below the edit row —
                // an "N more" overflow mark over blank space is just lost lines.
                let rows = bottom.saturating_sub(y).saturating_sub(rows_owed);
                if rows > 0 {
                    let picker_area = Rect {
                        x: area.x + 2,
                        y,
                        width: area.width.saturating_sub(2),
                        height: rows,
                    };
                    Picker::from_state(state, true).render(picker_area, buf, ctx);
                    y += rows;
                }
            }
            continue;
        }

        // The rail means focus: with the tab bar holding it, the cursor row
        // keeps its place but not the rail.
        let rail = if is_cursor && focused && filter.editor.is_none() {
            g.rail
        } else {
            " "
        };
        if row == filter.statements.len() {
            // The add row: the standing offer, dimmed until it is taken.
            let style = if is_cursor {
                Style::default().fg(ctx.accent)
            } else {
                Style::default().fg(ctx.dimmed)
            };
            Paragraph::new(Line::from(vec![
                Span::styled(rail, Style::default().fg(ctx.accent)),
                Span::styled(format!("add filter{}", g.ellipsis), style),
            ]))
            .render(row_area, buf);
            continue;
        }

        let statement = &filter.statements[row];
        // The conjunction binds this row to the one above, so the first shows none.
        let conjunction = if row == 0 {
            String::new()
        } else {
            format!("  {}", statement.logical_op.as_str().to_lowercase())
        };
        let mut style = if is_cursor {
            Style::default().fg(ctx.accent)
        } else {
            Style::default().fg(ctx.text_primary)
        };
        if is_cursor && filter.editor.is_none() {
            style = style.patch(ctx.highlight_style());
        }
        let text = format!(
            "{:<col$} {:<op$} {}{}",
            statement.column,
            statement.operator.as_str(),
            statement.value,
            conjunction,
            col = col_w,
            op = op_w,
        );
        Paragraph::new(Line::from(vec![
            Span::styled(rail, Style::default().fg(ctx.accent)),
            Span::styled(text, style),
        ]))
        .render(row_area, buf);
    }
}
