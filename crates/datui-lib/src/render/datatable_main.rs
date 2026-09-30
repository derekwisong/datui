//! Datatable main view: table content, input strip, sidebars (sort/filter, template, pivot/melt), export modal.

use crate::render::context::RenderContext;
use crate::render::datatable_view::{ActiveSidebar, DatatableLayout};
use crate::render::main_view::MainViewContent;
use crate::widgets::datatable::DataTable;
use crate::widgets::info::{DataTableInfo, InfoContext};
use crate::widgets::ui::HintBar;
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
    let prompt = app.input_type == Some(crate::InputType::Search);
    let error = if prompt {
        app.query_prompt_error()
    } else {
        app.data_table_state
            .as_ref()
            .and_then(|state| state.error.as_ref())
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
    } else if error.is_some() {
        6
    } else {
        3
    };

    let active_sidebar = ActiveSidebar::from_modals(
        app.info_modal.active,
        app.sort_filter_modal.active,
        app.template_modal.active,
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
    match &mut app.data_table_state {
        Some(state) => {
            let mut table_area = data_area;
            let breadcrumb_text = if state.is_drilled_down() {
                state.drilled_down_group_key.as_ref().map(|key_values| {
                    let key_columns = state
                        .drilled_down_group_key_columns
                        .as_deref()
                        .unwrap_or_default();
                    let parts: Vec<String> = key_columns
                        .iter()
                        .zip(key_values.iter())
                        .map(|(col, val)| format!("{col}={val}"))
                        .collect();
                    let g = crate::glyphs::get();
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
                .with_drift(
                    state.display_drift(table_area.height as usize),
                    state.drift_groups(),
                );
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
            if app.info_modal.active {
                let info_ctx = InfoContext {
                    path: app.path.as_deref(),
                    format: app.original_file_format,
                    parquet_metadata: app.parquet_metadata_cache.as_ref(),
                };
                let mut info_widget = DataTableInfo::new(state, info_ctx, &mut app.info_modal, ctx);
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

    if app.template_modal.active {
        crate::render::template_sidebar::render(
            sort_area,
            buf,
            &mut app.template_modal,
            app.active_template_id.as_deref(),
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
        let modal_height = 11.min(area.height);
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

    if app.copy_modal.active {
        // A commitment like export: compact and centered. The dialog holds
        // its rows, the spec and the footer; an open Picker earns the room
        // it drops into.
        let modal_width = (area.width * 3 / 4).min(46);
        let wanted = if app.copy_modal.picker.is_some() {
            14
        } else {
            app.copy_modal.row_order().len() as u16 + 5
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
    let chip_w = back.width_in(area.width.saturating_sub(12));
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
        back.render(
            Rect {
                x: area.x + area.width - chip_w,
                width: chip_w,
                ..area
            },
            buf,
        );
    }
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
