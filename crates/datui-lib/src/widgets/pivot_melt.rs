//! The Pivot & Melt builder: one Surface over the whole screen, the form on the
//! left and a live preview of the result on the right, stacked on a narrow
//! terminal. The form is a FormRow per field with the one Picker below the rows;
//! the preview is the first rows of the reshaped head, typed and colored as the
//! table draws them, under a line that says what it ran over and the shape.

use crate::app::modals::pivot_melt_modal::{
    PREVIEW_INPUT_ROWS, PREVIEW_WIDE_PIVOT, PivotMeltFocus, PivotMeltModal, PivotMeltTab,
    PreviewFrame, ReshapePreview,
};
use crate::app::pointer::{FieldId, Hit};
use crate::numfmt;
use crate::render::context::RenderContext;
use crate::render::footer::{Hint, registry_hint_in};
use crate::widgets::ui::{FormRow, FormValue, Picker, SectionRule, Surface};
use datui_cli::keys::Context;
use polars::prelude::{AnyValue, DataFrame};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

/// Where the value column starts, past the rail gutter: the longest label,
/// "Variable name:", plus two cells of air.
const LABEL_WIDTH: u16 = 16;

/// Content width from which the form and the preview sit side by side.
pub const SIDE_BY_SIDE: u16 = 96;

/// The form's width beside the preview.
const FORM_WIDTH: u16 = 46;

/// Cells between the form and the preview.
const GAP: u16 = 3;

/// The widest a preview column draws; a longer value is cut with the ellipsis.
const MAX_CELL: usize = 24;

/// Where the label column of the preview's lines ends.
const PANE_LABEL: usize = 9;

fn row_label(focus: PivotMeltFocus) -> &'static str {
    match focus {
        PivotMeltFocus::PivotIndex | PivotMeltFocus::MeltIndex => "Index:",
        PivotMeltFocus::PivotColumn | PivotMeltFocus::MeltColumns => "Columns:",
        PivotMeltFocus::PivotValue => "Values:",
        PivotMeltFocus::PivotAggregation => "Aggregate:",
        PivotMeltFocus::MeltStrategy => "Strategy:",
        PivotMeltFocus::MeltPattern => "Pattern:",
        PivotMeltFocus::MeltType => "Type:",
        PivotMeltFocus::MeltVariable => "Variable name:",
        PivotMeltFocus::MeltValue => "Value name:",
        PivotMeltFocus::TabBar => "",
    }
}

/// The builder's keys for the status footer, the two or three that act right now:
/// editing a row through the Picker, or walking and applying the form. Labels come
/// from the key registry.
pub fn hints(modal: &PivotMeltModal) -> Vec<Hint> {
    let form = |keys| registry_hint_in(Context::PivotMelt, Some("Form"), keys);
    let picker = |keys| registry_hint_in(Context::PivotMelt, Some("Picker"), keys);
    match &modal.picker {
        Some(_) if modal.is_multi_row(modal.focus) => {
            vec![picker("Space"), picker("Enter"), picker("Esc")]
        }
        Some(_) => vec![picker("Enter"), picker("Esc")],
        None if modal.is_picker_row(modal.focus) => {
            vec![form("Enter"), form("Space"), form("Esc")]
        }
        None if modal.focus == PivotMeltFocus::TabBar || modal.is_choice_row(modal.focus) => {
            vec![form("Enter"), form("← / →"), form("Esc")]
        }
        None => vec![form("Enter"), form("Esc")],
    }
}

/// Whether `?` types in the builder right now (a picker narrows, a text field
/// types), so help is F1.
pub fn question_types(modal: &PivotMeltModal) -> bool {
    modal.picker.is_some() || modal.is_text_row(modal.focus)
}

/// Draw the builder over `area`, the whole screen above the status footer, which
/// carries its keys ([`hints`]).
pub fn render(area: Rect, buf: &mut Buffer, modal: &mut PivotMeltModal, ctx: &RenderContext) {
    let content = Surface::new("Pivot & Melt").render(area, buf, ctx);
    if content.height < 4 || content.width < 10 {
        return;
    }
    let (form, preview) = split(content, modal);
    render_form(form, buf, modal, ctx);
    if let Some(preview) = preview {
        render_preview(preview, buf, modal, ctx);
    }
}

/// The form's area and the preview's: side by side when there is room, else the
/// form on top at the height it needs and the preview under it.
pub fn split(content: Rect, modal: &PivotMeltModal) -> (Rect, Option<Rect>) {
    if content.width >= SIDE_BY_SIDE {
        let form = Rect {
            width: FORM_WIDTH,
            ..content
        };
        let preview = Rect {
            x: content.x + FORM_WIDTH + GAP,
            width: content.width - FORM_WIDTH - GAP,
            ..content
        };
        return (form, Some(preview));
    }
    let form_height = form_height(modal).min(content.height);
    let form = Rect {
        height: form_height,
        ..content
    };
    // A blank row between them; the preview only where it has room for its lines.
    let rest = content.height.saturating_sub(form_height + 1);
    let preview = (rest >= 3).then(|| Rect {
        y: content.y + form_height + 1,
        height: rest,
        ..content
    });
    (form, preview)
}

/// Rows the form takes when stacked: the tab line and a blank, a row per field,
/// the open Picker under them, then a blank and the spec line.
fn form_height(modal: &PivotMeltModal) -> u16 {
    let rows = modal.row_order().len() as u16;
    let picker = modal
        .picker
        .as_ref()
        .map_or(0, |state| 1 + (state.filtered().len() as u16).clamp(1, 8));
    2 + rows + picker + 2
}

fn render_form(area: Rect, buf: &mut Buffer, modal: &mut PivotMeltModal, ctx: &RenderContext) {
    if area.height < 3 || area.width < 10 {
        return;
    }
    // Tab line: the active tab carries the accent, and the rail sits beside
    // its name while the tab bar holds focus — never beside a tab the
    // surface is not on. The slot is reserved either way, so nothing moves.
    let g = crate::glyphs::get();
    let pivot_tab = modal.active_tab == PivotMeltTab::Pivot;
    let on_tab_bar = modal.focus == PivotMeltFocus::TabBar;
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
        Span::styled(mark(pivot_tab), Style::default().fg(ctx.accent)),
        Span::styled("Pivot", tab_style(pivot_tab)),
        Span::styled(format!(" {}", g.rule), Style::default().fg(ctx.dimmed)),
        Span::styled(mark(!pivot_tab), Style::default().fg(ctx.accent)),
        Span::styled("Melt", tab_style(!pivot_tab)),
    ]);
    let tab_area = Rect { height: 1, ..area };
    let field = Some(FieldId::of::<PivotMeltModal>(PivotMeltFocus::TabBar));
    let current = usize::from(!pivot_tab);
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

    // The spec line sits on the form's last row.
    let spec_y = area.y + area.height - 1;

    modal
        .melt_pattern_input
        .set_focused(modal.focus == PivotMeltFocus::MeltPattern);
    modal
        .melt_variable_input
        .set_focused(modal.focus == PivotMeltFocus::MeltVariable);
    modal
        .melt_value_input
        .set_focused(modal.focus == PivotMeltFocus::MeltValue);

    // One FormRow per field; the chosen value is always echoed on the row, so
    // nothing is ambiguous when focus is elsewhere.
    let rows = modal.row_order();
    let mut y = area.y + 2;
    for &row in rows {
        if y >= spec_y {
            break;
        }
        let echo;
        let value = match row {
            PivotMeltFocus::PivotIndex => {
                echo = modal.index_columns.join(", ");
                echo_or_placeholder(&echo, "none")
            }
            PivotMeltFocus::PivotColumn => {
                echo_or_placeholder(modal.pivot_column.as_deref().unwrap_or(""), "none")
            }
            PivotMeltFocus::PivotValue => {
                echo_or_placeholder(modal.value_column.as_deref().unwrap_or(""), "none")
            }
            PivotMeltFocus::PivotAggregation => FormValue::Choice(modal.aggregation.as_str()),
            PivotMeltFocus::MeltIndex => {
                echo = modal.melt_index_columns.join(", ");
                echo_or_placeholder(&echo, "none")
            }
            PivotMeltFocus::MeltStrategy => FormValue::Choice(modal.melt_value_strategy.as_str()),
            PivotMeltFocus::MeltPattern => FormValue::Input(&modal.melt_pattern_input),
            PivotMeltFocus::MeltType => FormValue::Choice(modal.melt_type_filter.as_str()),
            PivotMeltFocus::MeltColumns => {
                echo = modal.melt_explicit_list.join(", ");
                echo_or_placeholder(&echo, "none")
            }
            PivotMeltFocus::MeltVariable => FormValue::Input(&modal.melt_variable_input),
            PivotMeltFocus::MeltValue => FormValue::Input(&modal.melt_value_input),
            PivotMeltFocus::TabBar => continue,
        };
        let row_area = Rect {
            y,
            height: 1,
            ..area
        };
        FormRow {
            label: row_label(row),
            value,
            focused: modal.focus == row,
            label_width: LABEL_WIDTH,
        }
        .render_picking(row_area, buf, ctx, modal.picker.is_some());
        crate::app::pointer::record_field::<PivotMeltModal>(row_area, row);
        y += 1;
    }

    // The focused row's Picker drops in below the rows and reaches down to the
    // spec line; the selection carries the rail while the list is up.
    if let Some(state) = &modal.picker {
        // It owns the keys even with no room to draw: the rows take no clicks.
        crate::app::pointer::record(area, Hit::Picker);
        let picker_y = y + 1;
        if picker_y < spec_y {
            let picker_area = Rect {
                x: area.x + 2,
                y: picker_y,
                width: area.width.saturating_sub(2),
                height: spec_y - picker_y,
            };
            let mut picker = Picker::from_state(state, true);
            if modal.is_multi_row(modal.focus) {
                let marks = state
                    .filtered()
                    .into_iter()
                    .map(|(_, item)| modal.is_marked(item))
                    .collect();
                picker = picker.marks(marks);
            }
            picker.render(picker_area, buf, ctx);
        }
    }

    // The full spec, echoed: a mis-aimed aggregation is catchable here, before
    // it runs. While the spec is incomplete, the line says what is missing.
    let spec = match modal.active_tab {
        PivotMeltTab::Pivot => modal.pivot_spec_line(g),
        PivotMeltTab::Melt => modal.melt_spec_line(),
    };
    let (text, style) = match spec {
        Ok(line) => (line, Style::default().fg(ctx.text_primary)),
        // Enter on the incomplete form lit the line up; edits dim it again.
        Err(gap) if modal.attention => (gap, Style::default().fg(ctx.warning)),
        Err(gap) => (gap, Style::default().fg(ctx.dimmed)),
    };
    let text = crate::glyphs::fit_cells(&text, area.width as usize, g.ellipsis).into_owned();
    Paragraph::new(text).style(style).render(
        Rect {
            y: spec_y,
            height: 1,
            ..area
        },
        buf,
    );
}

fn echo_or_placeholder<'a>(value: &'a str, placeholder: &'a str) -> FormValue<'a> {
    if value.is_empty() {
        FormValue::Placeholder(placeholder)
    } else {
        FormValue::Choice(value)
    }
}

/// What the preview runs over: all of a small view, or its first rows.
pub fn input_line(preview: &ReshapePreview) -> String {
    let n = numfmt::group_chrome;
    let whole = match &preview.input {
        Some(input) => input.whole.then(|| input.rows.height()),
        None => preview.view_rows.filter(|rows| *rows <= PREVIEW_INPUT_ROWS),
    };
    if let Some(rows) = whole {
        return format!("all {} rows", n(rows));
    }
    let of = preview
        .view_rows
        .map_or(String::new(), |rows| format!(" of {}", n(rows)));
    let unsorted = if preview.sorted { ", unsorted" } else { "" };
    format!("first {} rows{of}{unsorted}", n(PREVIEW_INPUT_ROWS))
}

/// The result's shape, `rows × columns`: exact when the head is the whole view;
/// otherwise `?` for what the head cannot say, and a pivot's columns as a floor.
pub fn shape_line(preview: &ReshapePreview, frame: &PreviewFrame) -> String {
    let g = crate::glyphs::get();
    let n = numfmt::group_chrome;
    let whole = preview.input.as_ref().is_some_and(|input| input.whole);
    let (rows, columns) = if whole {
        (n(frame.rows), n(frame.columns))
    } else if frame.new_columns.is_some() {
        ("?".to_string(), format!("{}+", n(frame.columns)))
    } else {
        // Each row of the view becomes one per value column.
        let rows = match (preview.view_rows, frame.melted_columns) {
            (Some(rows), Some(each)) => n(rows.saturating_mul(each)),
            _ => "?".to_string(),
        };
        (rows, n(frame.columns))
    };
    format!("{rows} rows {} {columns} columns", g.times)
}

/// A callout for a surprise: a pivot that makes many columns.
pub fn wide_pivot_callout(preview: &ReshapePreview, frame: &PreviewFrame) -> Option<String> {
    let new = frame.new_columns?;
    if new < PREVIEW_WIDE_PIVOT {
        return None;
    }
    let n = numfmt::group_chrome;
    let whole = preview.input.as_ref().is_some_and(|input| input.whole);
    let input_rows = preview
        .input
        .as_ref()
        .map_or(0, |input| input.rows.height());
    Some(if whole {
        format!("{} new columns", n(new))
    } else {
        format!(
            "{} new columns from {} rows; the whole view may make more",
            n(new),
            n(input_rows)
        )
    })
}

fn render_preview(area: Rect, buf: &mut Buffer, modal: &PivotMeltModal, ctx: &RenderContext) {
    if area.height == 0 || area.width < 10 {
        return;
    }
    let g = crate::glyphs::get();
    let preview = &modal.preview;
    SectionRule {
        title: "Preview",
        chip: None,
    }
    .render(Rect { height: 1, ..area }, buf, ctx);

    let fresh = preview
        .shown
        .as_ref()
        .filter(|(spec, _)| preview.wanted.as_ref() == Some(spec));
    let computing = preview.wanted.is_some() && fresh.is_none();
    let (result, callout): (String, Option<(String, Style)>) = match fresh {
        _ if preview.wanted.is_none() => ("-".to_string(), None),
        None => ("computing...".to_string(), None),
        Some((_, Ok(frame))) => (
            shape_line(preview, frame),
            wide_pivot_callout(preview, frame).map(|text| (text, Style::default().fg(ctx.warning))),
        ),
        // One line: a message's own line breaks become spaces.
        Some((_, Err(message))) => (
            "-".to_string(),
            Some((
                message.split_whitespace().collect::<Vec<_>>().join(" "),
                Style::default().fg(ctx.warning),
            )),
        ),
    };

    let width = area.width as usize;
    let pane_line = |label: &str, value: &str| {
        let value = crate::glyphs::fit_cells(value, width.saturating_sub(PANE_LABEL), g.ellipsis)
            .into_owned();
        Line::from(vec![
            Span::styled(
                format!("{label:<PANE_LABEL$}"),
                Style::default().fg(ctx.label),
            ),
            Span::styled(value, Style::default().fg(ctx.text_primary)),
        ])
    };
    let mut y = area.y + 1;
    let bottom = area.y + area.height;
    let mut line = |line: Line, y: &mut u16| {
        if *y < bottom {
            Paragraph::new(line).render(
                Rect {
                    y: *y,
                    height: 1,
                    ..area
                },
                buf,
            );
        }
        *y += 1;
    };
    line(pane_line("Input", &input_line(preview)), &mut y);
    line(pane_line("Result", &result), &mut y);
    // The callout's row is kept whether or not there is one, so the grid holds
    // still as answers arrive.
    let callout = callout.map(|(text, style)| {
        let text = format!("{} {text}", g.warning);
        Line::styled(
            crate::glyphs::fit_cells(&text, width, g.ellipsis).into_owned(),
            style,
        )
    });
    line(callout.unwrap_or_default(), &mut y);
    y += 1;

    // The last rows shown stay up, dimmed, while the next are computed.
    let grid = match &preview.shown {
        Some((_, Ok(frame))) if preview.wanted.is_some() => Some(&frame.head),
        _ => None,
    };
    if let Some(head) = grid
        && y < bottom
    {
        let grid_area = Rect {
            y,
            height: bottom - y,
            ..area
        };
        render_grid(grid_area, buf, head, computing, ctx);
    }
}

/// One column of the grid as drawn: name, type and cells, and its width.
struct GridColumn {
    name: String,
    dtype: String,
    color: ratatui::style::Color,
    right: bool,
    cells: Vec<Option<String>>,
    width: usize,
}

fn grid_columns(
    head: &DataFrame,
    rows: usize,
    limit: usize,
    width: u16,
    ctx: &RenderContext,
) -> Vec<GridColumn> {
    let g = crate::glyphs::get();
    let mut scratch = String::new();
    head.columns()
        .iter()
        .take(limit)
        .map(|column| {
            let name = column.name().to_string();
            let dtype = column.dtype();
            let fmt = ctx.number_format.formatter_for(&name, dtype);
            let cells: Vec<Option<String>> = (0..rows.min(column.len()))
                .map(|i| match column.get(i) {
                    Ok(AnyValue::Null) | Err(_) => None,
                    Ok(value) => Some(crate::exact::cell_preview(
                        &numfmt::format_any_value(&fmt, &value, &mut scratch),
                        g,
                        width,
                    )),
                })
                .collect();
            let type_label = crate::formats::column_types::dtype_label(dtype);
            let widest = cells
                .iter()
                .map(|c| c.as_deref().map_or(1, crate::glyphs::cell_width))
                .chain([
                    crate::glyphs::cell_width(&name),
                    if ctx.dtype_row {
                        crate::glyphs::display_width(&type_label)
                    } else {
                        0
                    },
                ])
                .max()
                .unwrap_or(1);
            GridColumn {
                color: ctx.type_color(dtype),
                right: ctx.number_format.align_numeric_right
                    && numfmt::is_right_aligned_dtype(dtype),
                name,
                dtype: type_label,
                cells,
                width: widest.clamp(1, MAX_CELL),
            }
        })
        .collect()
}

/// The preview's rows: header names in their type's color, the type row when the
/// table shows one, then the cells, nulls as the null glyph. Columns that do not
/// fit are counted at the end of the header.
fn render_grid(area: Rect, buf: &mut Buffer, head: &DataFrame, stale: bool, ctx: &RenderContext) {
    let g = crate::glyphs::get();
    let header_rows = if ctx.dtype_row { 2 } else { 1 };
    let body_rows = (area.height as usize).saturating_sub(header_rows);
    let total = area.width as usize;
    // Each column takes a cell and a gap at least: only those that could fit are
    // formatted.
    let columns = grid_columns(head, body_rows, total / 3 + 1, area.width, ctx);

    // Which columns fit, keeping room to say how many do not.
    let mut shown = 0;
    let mut used = 0;
    for (i, column) in columns.iter().enumerate() {
        let left = head.width() - i - 1;
        let more = if left > 0 {
            format!("  +{left}").len()
        } else {
            0
        };
        let needed = used + if i > 0 { 2 } else { 0 } + column.width;
        if needed + more > total && i > 0 {
            break;
        }
        used = needed.min(total);
        shown += 1;
    }
    let hidden = head.width() - shown;

    let style_for = |color| {
        if stale {
            Style::default().fg(ctx.dimmed)
        } else {
            Style::default().fg(color)
        }
    };
    let header_style = Style::default().bg(ctx.table_header_bg);
    for row in 0..header_rows.min(area.height as usize) {
        buf.set_style(
            Rect {
                y: area.y + row as u16,
                height: 1,
                ..area
            },
            header_style,
        );
    }
    let put = |buf: &mut Buffer, x: usize, y: u16, w: usize, text: &str, right: bool, style| {
        let text = crate::glyphs::fit_cells(text, w, g.ellipsis);
        let pad = w.saturating_sub(crate::glyphs::cell_width(&text));
        let x = area.x as usize + x + if right { pad } else { 0 };
        buf.set_stringn(x as u16, y, text.as_ref(), w, style);
    };
    let mut x = 0;
    for column in columns.iter().take(shown) {
        let w = column.width.min(total.saturating_sub(x));
        put(
            buf,
            x,
            area.y,
            w,
            &column.name,
            column.right,
            style_for(column.color)
                .bg(ctx.table_header_bg)
                .add_modifier(Modifier::BOLD),
        );
        if ctx.dtype_row && area.height > 1 {
            put(
                buf,
                x,
                area.y + 1,
                w,
                &column.dtype,
                column.right,
                Style::default().fg(ctx.dimmed).bg(ctx.table_header_bg),
            );
        }
        for (r, cell) in column.cells.iter().enumerate() {
            let y = area.y + (header_rows + r) as u16;
            if y >= area.y + area.height {
                break;
            }
            let (text, style) = match cell {
                Some(text) => (text.as_str(), style_for(ctx.text_primary)),
                None => (g.null, Style::default().fg(ctx.dimmed)),
            };
            let style = match ctx.alternate_row_color {
                Some(bg) if r % 2 == 1 => style.bg(bg),
                _ => style,
            };
            put(buf, x, y, w, text, column.right, style);
        }
        x += column.width + 2;
    }
    if hidden > 0 {
        let more = format!("+{hidden}");
        let at = (used + 2).min(total.saturating_sub(more.len()));
        put(
            buf,
            at,
            area.y,
            more.len().min(total),
            &more,
            false,
            Style::default().fg(ctx.dimmed).bg(ctx.table_header_bg),
        );
    }
    // Alternate rows tint the whole line, as the table's do.
    if let Some(bg) = ctx.alternate_row_color {
        for r in (1..body_rows.min(head.height())).step_by(2) {
            let y = area.y + (header_rows + r) as u16;
            if y < area.y + area.height {
                buf.set_style(
                    Rect {
                        y,
                        height: 1,
                        ..area
                    },
                    Style::default().bg(bg),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests;
