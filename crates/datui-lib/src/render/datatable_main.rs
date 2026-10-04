//! Datatable main view: table content, input strip, sidebars (sort/filter, views, pivot/melt), export modal.

use crate::render::context::RenderContext;
use crate::render::datatable_view::{ActiveSidebar, DatatableLayout};
use crate::render::main_view::MainViewContent;
use crate::widgets::datatable::DataTable;
use crate::widgets::info::{DataTableInfo, InfoContext};
use crate::widgets::ui::{HintBar, Working};
use crate::widgets::{copy, export, pivot_melt};
use ratatui::layout::Rect;
use ratatui::prelude::StatefulWidget;
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Clear, Paragraph, Widget};

/// Renders the datatable main view: layout, table content, input strip, sidebars, export modal.
pub fn render(
    area: Rect,
    main_area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &mut crate::App,
    ctx: &RenderContext,
) {
    let main_view_content = MainViewContent::from_app_state(
        app.analysis_modal.active,
        app.input_mode == crate::InputMode::Chart,
    );
    let input_strip_visible = main_view_content == MainViewContent::Datatable
        && app.input_mode == crate::InputMode::Editing;
    let prompt = app.input_type == Some(crate::InputType::Query);
    let finding = app.input_type == Some(crate::InputType::Find);
    let error = if prompt {
        app.query_prompt_error()
    } else if finding {
        app.find.error.clone()
    } else {
        app.data_table_state
            .as_ref()
            .and_then(|state| state.error())
            .map(crate::error_display::user_message_from_polars)
    };
    // The prompt grows with a long statement, its error and the column list, and
    // leaves the table a few rows to show what the query is over.
    const TABLE_KEEPS: u16 = 6;
    let input_strip_height = if !input_strip_visible {
        0
    } else if prompt {
        let room = main_area.height.saturating_sub(TABLE_KEEPS).max(5);
        crate::render::input_strip::plan(app, main_area.width, room, error.as_deref()).total()
    } else if finding {
        crate::render::input_strip::find_rows(error.is_some())
    } else if error.is_some() {
        6
    } else {
        3
    };

    let active_sidebar = ActiveSidebar::from_modals(
        app.info_modal.active,
        app.sort_filter_modal.active,
        app.view_modal.active,
        app.pivot_melt_modal.active,
    );

    let datatable_layout = DatatableLayout::compute(
        main_area,
        active_sidebar,
        input_strip_visible,
        input_strip_height,
        app.app_config.display.sidebar_width,
    );
    let data_area = datatable_layout.content_area;
    let sort_area = datatable_layout.sidebar_area.unwrap_or_default();

    let evidence_label = app.quality_evidence_label.clone();
    // Asked before the table is borrowed: the job records are the app's.
    let facts_reading = app.file_facts_reading();
    let facts_tab = app.info_facts_tab();
    let footer_expected = app.info_facts().is_some_and(|(_, facts)| facts.footer);
    let declared_types = app
        .opened_format()
        .is_some_and(|f| f.descriptor().declares_types);
    let find_cell = app.find_hit();
    let hex = app.hex_target().is_some();
    let query_reading = app.query_reading();
    let frame = app.throbber_frame as usize;
    match &mut app.data_table_state {
        Some(state) => {
            let mut table_area = data_area;
            let breadcrumb_text = if state.is_drilled_down() {
                state.drilled_group_key().map(|(key_columns, key_values)| {
                    let g = crate::glyphs::get();
                    // A tab or line break in a key would cut the line: marked, as
                    // in a cell.
                    let parts: Vec<String> = key_columns
                        .iter()
                        .zip(key_values.iter())
                        .map(|(col, val)| format!("{col}={}", crate::exact::cell_preview(val, g)))
                        .collect();
                    format!(
                        "{} Group: {}",
                        g.arrow_left,
                        parts.join(&format!(" {} ", g.middot))
                    )
                })
            } else {
                evidence_label.map(|label| format!("{} {label}", crate::glyphs::get().arrow_left))
            };
            if let Some(text) = breadcrumb_text {
                render_breadcrumb(
                    Rect {
                        height: 1.min(data_area.height),
                        ..data_area
                    },
                    buf,
                    &text,
                    ctx,
                );
                table_area = Rect {
                    y: data_area.y + 1.min(data_area.height),
                    height: data_area.height.saturating_sub(1),
                    ..data_area
                };
            }

            Clear.render(table_area, buf);
            let mut dt = DataTable::new()
                .with_colors(
                    ctx.table_header_bg,
                    ctx.table_header,
                    ctx.row_numbers,
                    ctx.column_separator,
                )
                .with_cell_padding(ctx.table_cell_padding)
                .with_screen_width(main_area.width)
                .with_alternate_row_bg(ctx.alternate_row_color)
                .with_binary_col(ctx.binary_col)
                .with_binary_columns(state.binary_column_names())
                .with_number_format(ctx.number_format.clone())
                .with_dtype_row(ctx.dtype_row)
                .with_selection_colors(
                    ctx.highlight_style(),
                    ctx.table_selected,
                    ctx.accent,
                    ctx.dimmed,
                )
                .with_cursor_styles(ctx.column_cursor_style(), ctx.cell_cursor_style())
                .with_drift(
                    state.display_drift(table_area.height as usize),
                    state.drift_groups(),
                )
                .with_find_cell(find_cell, ctx.find_match);
            if ctx.column_colors {
                dt = dt.with_column_type_colors(
                    ctx.str_col,
                    ctx.int_col,
                    ctx.float_col,
                    ctx.bool_col,
                    ctx.temporal_col,
                );
            }
            StatefulWidget::render(dt, table_area, buf, state);
            if query_reading {
                // Drawn still, so the table keeps its size: the rows are the view the
                // query replaces, under columns it may have changed.
                Working {
                    text: "Running query...",
                    frame,
                }
                .render_centered(table_area, buf, ctx);
            }
            if app.info_modal.active {
                let facts =
                    crate::App::facts_shown(&app.file_facts, app.dataset_generation, facts_reading);
                let info_ctx = InfoContext {
                    format: app.original_file_format,
                    declared_types,
                    facts,
                    facts_tab,
                    footer_expected,
                };
                let mut info_widget = DataTableInfo::new(state, info_ctx, &mut app.info_modal, ctx);
                info_widget.hex = hex;
                info_widget.codebook = app.codebook.as_deref();
                info_widget.render(sort_area, buf);
            }
        }
        None => {
            // No dataset at all: a first load that failed, leaving its error modal over
            // an empty screen. A load still in flight never reaches here — it draws
            // `loading_view` instead.
        }
    }

    if app.input_mode == crate::InputMode::Editing {
        let input_area = datatable_layout.input_strip_area.unwrap_or_else(|| {
            let y = main_area
                .y
                .saturating_add(main_area.height.saturating_sub(input_strip_height));
            Rect {
                x: area.x,
                y,
                width: area.width,
                height: input_strip_height.min(main_area.height),
            }
        });
        crate::render::input_strip::render(input_area, buf, app, error.as_deref(), ctx);
    }

    if app.sort_filter_modal.active {
        crate::render::sort_filter_sidebar::render(sort_area, buf, &mut app.sort_filter_modal, ctx);
    }

    if app.view_modal.active {
        crate::render::view_sidebar::render(
            sort_area,
            buf,
            &mut app.view_modal,
            app.active_view_id.as_deref(),
            ctx,
        );
    }

    if app.pivot_melt_modal.active {
        pivot_melt::render(sort_area, buf, &mut app.pivot_melt_modal, ctx);
    }

    if app.export_modal.active {
        // A commitment, so a compact centered dialog: the format list plus a
        // row per option, never scaling with the terminal.
        let modal_width = (area.width * 3 / 4).min(66);
        let modal_height = 13.min(area.height);
        let modal_x = (area.width.saturating_sub(modal_width)) / 2;
        let modal_y = (area.height.saturating_sub(modal_height)) / 2;
        let modal_area = Rect {
            x: modal_x,
            y: modal_y,
            width: modal_width,
            height: modal_height,
        };
        export::render_export_modal(modal_area, buf, &mut app.export_modal, ctx);
    }

    if app.inspector_modal.active
        && app.input_mode == crate::InputMode::Inspect
        && let Some(state) = app.data_table_state.as_ref()
    {
        // A takeover: the row's fields want the width a long value reads at, and
        // the table under it holds the cursor the inspector moves.
        crate::widgets::inspector::render(
            main_area,
            buf,
            &mut app.inspector_modal,
            state,
            app.codebook.as_deref(),
            ctx,
        );
    }

    if app.input_mode == crate::InputMode::GoToColumn {
        render_go_to_column(data_area, buf, &app.go_to_column, ctx);
    }

    if app.input_mode == crate::InputMode::PickFormat {
        render_picker(
            data_area,
            buf,
            &app.format_picker,
            ("Format", "Read", "No format matches"),
            ctx,
        );
    }

    if app.copy_modal.active {
        // A commitment like export: compact and centered. The dialog holds
        // its rows, the spec and the footer; an open Picker earns the room
        // it drops into.
        let modal_width = (area.width * 3 / 4).min(46);
        let wanted = if app.copy_modal.picker.is_some() {
            15
        } else {
            app.copy_modal.row_order().len() as u16 + 6
        };
        let modal_height = wanted.min(area.height);
        let modal_area = Rect {
            x: (area.width.saturating_sub(modal_width)) / 2,
            y: (area.height.saturating_sub(modal_height)) / 2,
            width: modal_width,
            height: modal_height,
        };
        copy::render_copy_modal(modal_area, buf, &mut app.copy_modal, ctx);
    }
}

/// The drill-down breadcrumb: one line in the header tier above the table,
/// saying which rows these are, with the way back as a chip on the right.
pub(crate) fn render_breadcrumb(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    text: &str,
    ctx: &RenderContext,
) {
    if area.height == 0 || area.width == 0 {
        return;
    }
    // The analysis header's tier, so a drill-down reads like the header over a
    // tool's result.
    let style = Style::default().bg(ctx.controls_bg).fg(ctx.table_header);
    buf.set_style(area, style);
    let back = HintBar::from_ctx(ctx).hint("Esc", "Back");
    let chip_w = back.flush_width_in(area.width.saturating_sub(12));
    let text_w = area.width.saturating_sub(chip_w + 1);
    Paragraph::new(crate::render::loading_view::truncate(text, text_w as usize))
        .style(style.add_modifier(Modifier::BOLD))
        .render(
            Rect {
                width: text_w,
                ..area
            },
            buf,
        );
    if chip_w > 0 {
        back.render_flush(
            Rect {
                x: area.x + area.width - chip_w,
                width: chip_w,
                ..area
            },
            buf,
        );
    }
}

/// The column picker: a short list over the table, which stays in view behind it so
/// the jump is seen landing. Its height is capped and the list scrolls inside it.
/// Sized by every column rather than the ones the filter admits, so typing moves
/// nothing.
fn render_go_to_column(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    picker: &crate::widgets::ui::PickerState,
    ctx: &RenderContext,
) {
    render_picker(
        area,
        buf,
        picker,
        ("Go to Column", "Go", "No column matches"),
        ctx,
    );
}

/// A short pick-one list over the table: `(title, what Enter does, what an empty
/// narrowing says)`.
pub(crate) fn render_picker(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    picker: &crate::widgets::ui::PickerState,
    (title, enter, none): (&str, &str, &str),
    ctx: &RenderContext,
) {
    use ratatui::text::{Line, Span};
    let all = picker.items();
    let widest = all
        .iter()
        .map(|item| crate::glyphs::display_width(item))
        .max()
        .unwrap_or(0) as u16;
    let width = (widest + 6).clamp(30, 48).min(area.width);
    // Frame, filter line, the blank above the footer, footer, and up to ten names.
    let height = (all.len().max(1) as u16 + 5).min(15).min(area.height);
    let items = picker.filtered();
    if width < 4 || height < 4 {
        return;
    }
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 3,
        width,
        height,
    };
    let footer = HintBar::from_ctx(ctx).hints(&[("Enter", enter), ("Esc", "Cancel")]);
    let inner = crate::widgets::ui::Surface::new(title)
        .footer(&footer)
        .render(popup, buf, ctx);
    let filter = if picker.filter.is_empty() {
        Line::from(Span::styled(
            "type to narrow",
            Style::default().fg(ctx.dimmed),
        ))
    } else {
        Line::from(Span::raw(picker.filter.clone()))
    };
    Paragraph::new(filter).render(Rect { height: 1, ..inner }, buf);
    let list = Rect {
        y: inner.y + 1,
        height: inner.height.saturating_sub(1),
        ..inner
    };
    if items.is_empty() {
        Paragraph::new(Span::styled(none, Style::default().fg(ctx.dimmed)))
            .render(Rect { height: 1, ..list }, buf);
        return;
    }
    crate::widgets::ui::Picker::from_state(picker, true).render(list, buf, ctx);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::buffer::Buffer;

    fn rows(buf: &Buffer) -> Vec<String> {
        let area = buf.area;
        (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect()
    }

    /// One line, no box: the text on the left, the way back on the right, and
    /// nothing drawn on the row below it.
    #[test]
    fn the_breadcrumb_is_one_plain_line() {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, 60, 3);
        let mut buf = Buffer::empty(area);
        render_breadcrumb(
            Rect { height: 1, ..area },
            &mut buf,
            "<- Group: department=Engineering",
            &ctx,
        );
        let rows = rows(&buf);
        assert!(
            rows[0].starts_with("<- Group: department=Engineering"),
            "{rows:?}"
        );
        assert!(rows[0].trim_end().ends_with("Back"), "{rows:?}");
        assert!(rows[0].contains("Esc"), "{rows:?}");
        for row in &rows[1..] {
            assert!(row.trim().is_empty(), "a second row was drawn: {rows:?}");
        }
        let g = crate::glyphs::get();
        for glyph in [
            g.border.top_left,
            g.border.bottom_left,
            g.border.vertical_left,
        ] {
            assert!(
                !rows[0].contains(glyph),
                "a border around one line: {rows:?}"
            );
        }
    }

    /// Too long for the row, the text is cut with a mark and the way back stays.
    #[test]
    fn a_long_breadcrumb_is_cut_and_keeps_the_way_back() {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, 40, 1);
        let mut buf = Buffer::empty(area);
        let long = format!("<- Group: {}", "x".repeat(80));
        render_breadcrumb(area, &mut buf, &long, &ctx);
        let row = &rows(&buf)[0];
        assert!(row.contains(crate::glyphs::get().ellipsis), "{row:?}");
        assert!(row.contains("Esc") && row.contains("Back"), "{row:?}");
    }
}
