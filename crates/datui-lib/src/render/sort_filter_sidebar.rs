//! Sort & Filter sidebar: what is in effect first — the sorts, then the filters,
//! each a row, the filters edited inline through Pickers — and a Columns tab with
//! every per-column property in one flat list. Built on the `widgets::ui` kit; the
//! rail marks focus.

use crate::app::modals::filter_modal::{FilterEditStep, FilterModal};
use crate::app::modals::sort_filter_modal::{SortFilterField, SortFilterModal, SortFilterTab};
use crate::app::pointer::{FieldId, Hit};
use crate::render::context::RenderContext;
use crate::widgets::column_widths::WidthChoice;
use crate::widgets::ui::{HintBar, Picker, SectionRule, Surface};
use datui_cli::keys::Context;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

/// Render the Sort & Filter sidebar into the given area.
pub fn render(area: Rect, buf: &mut Buffer, modal: &mut SortFilterModal, ctx: &RenderContext) {
    let columns_tab = modal.active_tab == SortFilterTab::Columns;
    let editing = modal.filter.editor.is_some() || modal.sort_picker.is_some();

    // The footer names what Enter does right now: apply, or — while the filter
    // editor owns Enter — the apply key that still works. That is Ctrl+J, not
    // Ctrl+Enter: without the keyboard-enhancement protocol many terminals send
    // Ctrl+Enter as a plain Enter.
    let footer = HintBar::from_ctx(ctx)
        .screen(Context::SortFilter)
        .group("Sidebar");
    let footer = if editing {
        footer.key("Ctrl+J").weight(2)
    } else {
        footer
            .key("Enter")
            .weight(3)
            .key("Tab")
            .weight(1)
            .key("Esc")
            .weight(4)
    };
    let staged = modal.sort.has_unapplied_changes || modal.filter.has_unapplied_changes();
    let footer = if !editing && staged {
        // Staged edits give the apply chip a quiet accent: something is waiting.
        footer.accent("Enter")
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
    let on_tab_bar = modal.focus == SortFilterField::TabBar;
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
        Span::styled(mark(!columns_tab), Style::default().fg(ctx.accent)),
        Span::styled("Sort & Filter", tab_style(!columns_tab)),
        Span::styled(format!(" {}", g.rule), Style::default().fg(ctx.dimmed)),
        Span::styled(mark(columns_tab), Style::default().fg(ctx.accent)),
        Span::styled("Columns", tab_style(columns_tab)),
    ]);
    let tab_area = Rect {
        height: 1,
        ..content
    };
    let field = Some(FieldId::of::<SortFilterModal>(SortFilterField::TabBar));
    let current = usize::from(columns_tab);
    crate::app::pointer::record_spans(
        tab_area,
        &tab_line,
        [1, 4]
            .into_iter()
            .enumerate()
            .map(|(index, span)| {
                let hit = Hit::Option {
                    field: field.clone(),
                    index,
                    current,
                };
                (span, hit)
            })
            .collect(),
    );
    Paragraph::new(tab_line).render(tab_area, buf);

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
    if columns_tab {
        render_columns_tab(body, buf, modal, ctx);
    } else {
        render_in_effect(body, buf, modal, ctx);
    }
    // Why the last key did nothing, or a value that does not read as its column's
    // type, takes the hint line until the next key, so arriving and leaving move
    // nothing.
    if let Some(status) = &modal.sort.status {
        Paragraph::new(status.as_str())
            .style(Style::default().fg(ctx.warning))
            .render(hints_area, buf);
        return;
    }
    let bar = HintBar::from_ctx(ctx).screen(Context::SortFilter);
    let editor = || bar.clone().group("Filter editor");
    let sidebar = || bar.clone().group("Sidebar");
    // What a row in effect takes besides Space: its place, and leaving.
    let in_effect = |bar: HintBar| {
        bar.group("In effect")
            .key("[ / ]")
            .weight(3)
            .key("d")
            .weight(2)
            .key("C")
            .weight(1)
    };
    let hints = match (modal.focus, modal.filter.editor.as_ref().map(|e| e.step)) {
        (_, Some(FilterEditStep::Value)) => editor()
            .key_as("Enter", "Save")
            .weight(2)
            .key("Esc")
            .weight(1),
        (_, Some(_)) => editor()
            .key("(type)")
            .weight(1)
            .key("Enter")
            .weight(3)
            .key("Esc")
            .weight(2),
        _ if modal.sort_picker.is_some() => editor()
            .key("(type)")
            .weight(1)
            .key_as("Enter", "Add")
            .weight(3)
            .key("Esc")
            .weight(2),
        (SortFilterField::TabBar, _) => sidebar().key_as("← / →", "Tabs").weight(1),
        (SortFilterField::Sort(_), _) => in_effect(sidebar().key_as("Space", "Flip").weight(4)),
        // The first filter joins nothing, so it has no and/or to toggle.
        (SortFilterField::Filter(0), _) => in_effect(sidebar().key_as("Space", "Edit").weight(5)),
        (SortFilterField::Filter(_), _) => in_effect(
            sidebar()
                .key_as("Space", "Edit")
                .weight(5)
                .key_as("← / →", "And/Or")
                .weight(4),
        ),
        (SortFilterField::AddSort | SortFilterField::AddFilter, _) => sidebar()
            .key_as("Space", "Add")
            .weight(2)
            .group("In effect")
            .key("C")
            .weight(1),
        (SortFilterField::Find, _) => bar.group("Columns").key_as("(type)", "Find").weight(1),
        (SortFilterField::Column(_), _) => bar
            .group("Columns")
            .key("Space")
            .weight(7)
            .key("1-9")
            .weight(3)
            .key("L")
            .weight(6)
            .key("v")
            .weight(4)
            .key("< / >")
            .weight(5)
            .key("f")
            .weight(2)
            .key("C")
            .weight(1),
    };
    hints.render_flush(hints_area, buf);
}

/// The Sort & Filter tab: the sort's keys in order and the add-sort row under a
/// rule, then the filters and their add row under another. The add-sort Picker
/// drops in below its row.
fn render_in_effect(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut SortFilterModal,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    if area.height == 0 {
        return;
    }
    let entries = modal.sort.sort_entries();
    let filter_focused = matches!(
        modal.focus,
        SortFilterField::Filter(_) | SortFilterField::AddFilter
    ) || modal.filter.editor.is_some();

    let count = entries.len().to_string();
    SectionRule {
        title: "Sort",
        chip: (!entries.is_empty()).then_some(count.as_str()),
    }
    .render(Rect { height: 1, ..area }, buf, ctx);

    // The sort section takes what its rows need (and the add-sort Picker, while
    // open), leaving the filters a blank row, their rule and two rows; the rows
    // scroll to keep focus in view.
    let bottom = area.y + area.height;
    let sort_rows = entries.len() + 1;
    let picker_want = if modal.sort_picker.is_some() { 8 } else { 0 };
    // An open Picker takes the filters' room too: it is what the keys are on.
    let room = if modal.sort_picker.is_some() {
        usize::from(area.height.saturating_sub(1))
    } else {
        usize::from(area.height.saturating_sub(1)).saturating_sub(4)
    };
    let shown = (sort_rows + picker_want).min(room).max(1);
    let list_rows = sort_rows.min(shown.saturating_sub(picker_want).max(1));
    let focus_row = match modal.focus {
        SortFilterField::Sort(i) => i,
        SortFilterField::AddSort => entries.len(),
        _ => 0,
    };
    let offset = focus_row.saturating_sub(list_rows - 1);
    let name_room = usize::from(area.width).saturating_sub(6);
    let mut y = area.y + 1;
    for row in offset..(offset + list_rows).min(sort_rows) {
        if y >= bottom {
            break;
        }
        let row_area = Rect {
            y,
            height: 1,
            ..area
        };
        y += 1;
        let field = if row == entries.len() {
            SortFilterField::AddSort
        } else {
            SortFilterField::Sort(row)
        };
        crate::app::pointer::record_field::<SortFilterModal>(row_area, field);
        let focused = match modal.focus {
            SortFilterField::Sort(i) => i == row,
            SortFilterField::AddSort => row == entries.len(),
            _ => false,
        } && modal.sort_picker.is_none();
        let rail = if focused { g.rail } else { " " };
        if row == entries.len() {
            let style = if focused {
                Style::default().fg(ctx.accent)
            } else {
                Style::default().fg(ctx.dimmed)
            };
            Paragraph::new(Line::from(vec![
                Span::styled(rail, Style::default().fg(ctx.accent)),
                Span::styled(format!("add sort{}", g.ellipsis), style),
            ]))
            .render(row_area, buf);
            continue;
        }
        let column = &modal.sort.columns[entries[row]];
        let arrow = if column.sort_descending {
            g.sort_desc
        } else {
            g.sort_asc
        };
        let name = crate::glyphs::fit_cells(&column.name, name_room, g.ellipsis);
        let mut style = if focused {
            Style::default().fg(ctx.accent)
        } else {
            Style::default().fg(ctx.text_primary)
        };
        if focused {
            style = style.patch(ctx.highlight_style());
        }
        Paragraph::new(Line::from(vec![
            Span::styled(rail, Style::default().fg(ctx.accent)),
            Span::styled(format!("{:>2} {arrow} {name}", row + 1), style),
        ]))
        .style(if focused {
            ctx.highlight_style()
        } else {
            Style::default()
        })
        .render(row_area, buf);
    }
    if let Some(picker) = &modal.sort_picker {
        // It owns the keys even with no room to draw: the rows take no clicks.
        crate::app::pointer::record(area, Hit::Picker);
        let rows = (area.y + 1 + shown as u16).min(bottom).saturating_sub(y);
        if rows > 0 {
            Picker::from_state(picker, true).render(
                Rect {
                    x: area.x + 2,
                    y,
                    width: area.width.saturating_sub(2),
                    height: rows,
                },
                buf,
                ctx,
            );
        }
    }

    // The filters, a blank row below the sort.
    let rule_y = area.y + 1 + shown as u16 + 1;
    if rule_y >= bottom || modal.sort_picker.is_some() {
        return;
    }
    let count = modal.filter.statements.len().to_string();
    SectionRule {
        title: "Filters",
        chip: (!modal.filter.statements.is_empty()).then_some(count.as_str()),
    }
    .render(
        Rect {
            y: rule_y,
            height: 1,
            ..area
        },
        buf,
        ctx,
    );
    let filters_area = Rect {
        y: rule_y + 1,
        height: bottom.saturating_sub(rule_y + 1),
        ..area
    };
    render_filters(filters_area, buf, &mut modal.filter, filter_focused, ctx);
}

/// Cells before a column's name in the Columns list: the rail, a space, the lock
/// and the sort.
const LIST_LEAD: usize = 13;
/// Cells for the width at the end of a Columns row, its leading space included,
/// while any column has a width set.
const WIDTH_FIELD: usize = 6;

/// The Columns tab: find row, header, one row per column.
fn render_columns_tab(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut SortFilterModal,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    let on_find = modal.focus == SortFilterField::Find;
    let on_list = matches!(modal.focus, SortFilterField::Column(_));

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
    crate::app::pointer::record_field::<SortFilterModal>(
        Rect { height: 1, ..area },
        SortFilterField::Find,
    );
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
    // The width sits at the end of the row, so a narrow sidebar takes its room from
    // the name's tail rather than moving the name. Its field is there only while a
    // column has a width to show: in a narrow sidebar the names need the room.
    let width_field = if modal
        .sort
        .columns
        .iter()
        .any(|c| c.width != WidthChoice::Auto)
    {
        WIDTH_FIELD
    } else {
        0
    };
    let name_room = usize::from(area.width).saturating_sub(LIST_LEAD + width_field);
    let width_heading = if width_field > 0 { "Width" } else { "" };
    let header = format!(
        "  {:<5}{:<6}{:<name_room$}{:>width_field$}",
        "Lock", "Sort", "Column", width_heading
    );
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
    let selected = modal
        .sort
        .table_state
        .selected()
        .unwrap_or(0)
        .min(filtered.len().saturating_sub(1));
    let height = list_area.height as usize;
    let window = column_window(selected, filtered.len(), height);
    let mut more = |row: usize, count: usize| {
        let row_area = Rect {
            y: list_area.y + row as u16,
            height: 1,
            ..list_area
        };
        Paragraph::new(format!("  {} {count} more", g.ellipsis))
            .style(Style::default().fg(ctx.dimmed))
            .render(row_area, buf);
    };
    if window.above > 0 {
        more(0, window.above);
    }
    if window.below > 0 {
        more(height - 1, window.below);
    }
    let lead = usize::from(window.above > 0);
    for k in 0..window.shown {
        let i = window.first + k;
        let row = lead + k;
        let row_area = Rect {
            y: list_area.y + row as u16,
            height: 1,
            ..list_area
        };
        let is_cursor = i == selected;
        crate::app::pointer::record_field::<SortFilterModal>(row_area, SortFilterField::Column(i));
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
        // A width set by hand, or a fit waiting for the table, is state; automatic
        // is the default and stays blank.
        let width = match column.width {
            WidthChoice::Auto => String::new(),
            WidthChoice::Manual(cells) => cells.to_string(),
            WidthChoice::Fit => "fit".to_string(),
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
        let name = crate::glyphs::fit_cells(
            &column.name,
            name_room.saturating_sub(crate::glyphs::cell_width(&hidden)),
            g.ellipsis,
        );
        let pad = name_room
            .saturating_sub(crate::glyphs::cell_width(&name) + crate::glyphs::cell_width(&hidden));
        let text = format!(
            " {:<5}{:<6}{name}{hidden}{}{:>width_field$}",
            lock,
            sort,
            " ".repeat(pad),
            width
        );
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
    modal.sort.page_rows = window.shown.max(1);
}

/// Which of `total` columns a list `rows` tall shows around `selected`.
#[derive(Debug, PartialEq, Eq)]
struct ColumnWindow {
    /// The first column shown, and how many.
    first: usize,
    shown: usize,
    /// Columns out of view above and below, each counted on a row of its own.
    above: usize,
    below: usize,
}

/// The window keeps the cursor in view and never on a count: a list longer than
/// its rows gives its first row to what is above and its last to what is below,
/// whenever there is any.
fn column_window(selected: usize, total: usize, rows: usize) -> ColumnWindow {
    if total <= rows || rows < 3 {
        let first = selected.saturating_sub(rows.saturating_sub(1));
        return ColumnWindow {
            first,
            shown: total.saturating_sub(first).min(rows),
            above: 0,
            below: 0,
        };
    }
    // At the top: the last row counts what is below.
    if selected < rows - 1 {
        return ColumnWindow {
            first: 0,
            shown: rows - 1,
            above: 0,
            below: total - (rows - 1),
        };
    }
    // At the bottom: the first row counts what is above.
    if selected >= total - (rows - 1) {
        let first = total - (rows - 1);
        return ColumnWindow {
            first,
            shown: rows - 1,
            above: first,
            below: 0,
        };
    }
    // Between: both ends count, the cursor on the last row between them.
    let first = selected + 1 - (rows - 2);
    ColumnWindow {
        first,
        shown: rows - 2,
        above: first,
        below: total - (first + rows - 2),
    }
}

/// The filters: one row per statement, the add row, and the inline editor. The
/// cursor row carries the rail and the tint only while the filters have focus.
fn render_filters(
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
        .map(|s| crate::glyphs::display_width(&filter.column_label(s)))
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
        let is_cursor = focused && row == filter.cursor;
        let under_edit = filter
            .editor
            .as_ref()
            .is_some_and(|editor| editor.editing == Some(row))
            || (filter.editor.as_ref().is_some_and(|e| e.editing.is_none())
                && row == filter.statements.len());

        if under_edit {
            // While a filter is edited in place, it alone takes clicks.
            crate::app::pointer::record(row_area, Hit::Editor);
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
                Some(i) if step != FilterEditStep::Column => {
                    filter.available_columns.get(i).cloned().unwrap_or_else(|| {
                        crate::app::modals::filter_modal::ANY_COLUMN_LABEL.to_string()
                    })
                }
                _ => format!("{}{}", editor.column.filter, g.cursor),
            };
            let operator_text = if step == FilterEditStep::Operator {
                format!("{}{}", editor.operator.filter, g.cursor)
            } else {
                editor
                    .selected_operator()
                    .map(|op| op.as_str().to_string())
                    .unwrap_or_default()
            };
            // The open picker's line has the one rail; typing the value, there is
            // no picker and the row keeps it.
            let rail = if step == FilterEditStep::Value {
                g.rail
            } else {
                " "
            };
            let mut spans = vec![
                Span::styled(rail, Style::default().fg(ctx.accent)),
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

        let rail = if is_cursor && filter.editor.is_none() {
            g.rail
        } else {
            " "
        };
        let field = if row == filter.statements.len() {
            SortFilterField::AddFilter
        } else {
            SortFilterField::Filter(row)
        };
        crate::app::pointer::record_field::<SortFilterModal>(row_area, field);
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
            filter.column_label(statement),
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

#[cfg(test)]
mod tests {
    /// A long Columns list counts what is out of view at either end, on rows of
    /// their own, and the cursor is always shown.
    #[test]
    fn the_columns_list_counts_both_ends() {
        use super::{ColumnWindow, column_window};
        let w = |first, shown, above, below| ColumnWindow {
            first,
            shown,
            above,
            below,
        };
        assert_eq!(column_window(0, 52, 10), w(0, 9, 0, 43));
        assert_eq!(column_window(8, 52, 10), w(0, 9, 0, 43));
        assert_eq!(column_window(9, 52, 10), w(2, 8, 2, 42));
        assert_eq!(column_window(51, 52, 10), w(43, 9, 43, 0));
        assert_eq!(column_window(3, 5, 10), w(0, 5, 0, 0));
        for selected in 0..52 {
            let window = column_window(selected, 52, 10);
            assert!(window.first <= selected && selected < window.first + window.shown);
            assert_eq!(window.above + window.shown + window.below, 52, "{selected}");
            let rows = window.shown + usize::from(window.above > 0) + usize::from(window.below > 0);
            assert!(rows <= 10, "{selected}");
        }
    }
    use super::*;
    use crate::app::modals::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    use crate::app::modals::sort_modal::SortColumn;

    fn modal() -> SortFilterModal {
        let mut m = SortFilterModal::new();
        m.sort.columns = ["restaurant", "calories", "protein"]
            .iter()
            .enumerate()
            .map(|(i, name)| SortColumn {
                name: name.to_string(),
                sort_order: (i == 1).then_some(1),
                sort_descending: true,
                display_order: i,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
                width: WidthChoice::Auto,
                shown_width: None,
            })
            .collect();
        m.filter.statements = vec![FilterStatement {
            columns: Vec::new(),
            column: "protein".to_string(),
            operator: FilterOperator::GtEq,
            value: "40".to_string(),
            logical_op: LogicalOperator::And,
        }];
        m.filter.available_columns = vec!["restaurant".into(), "calories".into()];
        let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default())
            .expect("theme");
        m.open(10, &theme, Some("restaurant"));
        m
    }

    fn painted(m: &mut SortFilterModal, width: u16, height: u16) -> Vec<String> {
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        render(area, &mut buf, m, &RenderContext::for_test());
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect()
            })
            .collect()
    }

    /// What is in effect, top to bottom: the sort's keys, its add row, then the
    /// filters and theirs; the focused row carries the rail.
    #[test]
    fn the_sidebar_lists_what_is_in_effect() {
        let g = crate::glyphs::get();
        let mut m = modal();
        let rows = painted(&mut m, 40, 20);
        let at = |needle: &str| {
            rows.iter()
                .position(|r| r.contains(needle))
                .unwrap_or_else(|| panic!("{needle:?} not drawn: {rows:#?}"))
        };
        let sort = at(&format!("1 {} calories", g.sort_desc));
        assert!(rows[sort].contains(g.rail), "focus opens on the first sort");
        assert_eq!(at("add sort"), sort + 1);
        assert!(at("Filters") > sort + 1);
        assert!(at("protein") > at("Filters"));
        assert!(at("add filter") > at("protein"));
        assert!(rows.iter().any(|r| r.contains("Flip")), "{rows:#?}");
    }

    /// A staged filter waits for Enter as a staged sort does: the apply chip's
    /// label takes the accent until the filters match those in effect.
    #[test]
    fn a_staged_filter_accents_apply() {
        let ctx = RenderContext::for_test();
        let apply_fg = |m: &mut SortFilterModal| {
            let area = Rect::new(0, 0, 40, 20);
            let mut buf = Buffer::empty(area);
            render(area, &mut buf, m, &ctx);
            let y = area.height - 2;
            let x = (0..area.width - 5)
                .find(|&x| (x..x + 5).map(|x| buf[(x, y)].symbol()).collect::<String>() == "Apply")
                .expect("the apply chip");
            buf[(x, y)].fg
        };
        let mut m = modal();
        m.filter.applied = m.filter.statements.clone();
        assert_ne!(apply_fg(&mut m), ctx.accent, "nothing staged");
        m.filter.statements[0].value = "50".to_string();
        assert_eq!(apply_fg(&mut m), ctx.accent, "a filter staged");
        m.filter.statements = m.filter.applied.clone();
        assert_ne!(apply_fg(&mut m), ctx.accent, "back as applied");
    }

    /// The add-sort Picker drops in under its row, at any size without a panic.
    #[test]
    fn the_add_sort_picker_opens_in_place() {
        let mut m = modal();
        m.focus = SortFilterField::AddSort;
        m.open_sort_picker();
        for (w, h) in [(40, 20), (30, 12), (24, 8), (12, 4)] {
            let rows = painted(&mut m, w, h);
            if h >= 12 {
                assert!(rows.iter().any(|r| r.contains("restaurant")), "{rows:#?}");
            }
        }
    }
}
