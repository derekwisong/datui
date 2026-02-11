//! Datatable main view: table content, input strip, sidebars (sort/filter, template, pivot/melt), export modal.

use crate::render::context::RenderContext;
use crate::render::datatable_view::{ActiveSidebar, DatatableLayout};
use crate::render::main_view::MainViewContent;
use crate::widgets::datatable::DataTable;
use crate::widgets::info::{DataTableInfo, InfoContext};
use crate::widgets::{export, pivot_melt};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::StatefulWidget;
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, BorderType, Borders, Clear, Paragraph, Widget};

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
    let (has_error, err_msg) = match &app.data_table_state {
        Some(state) => match &state.error {
            Some(e) => (true, crate::error_display::user_message_from_polars(e)),
            None => (false, String::new()),
        },
        None => (false, String::new()),
    };
    let input_strip_height = if input_strip_visible {
        if app.input_type == Some(crate::InputType::Search) {
            if has_error {
                9
            } else {
                5
            }
        } else if has_error {
            6
        } else {
            3
        }
    } else {
        0
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

    match &mut app.data_table_state {
        Some(state) => {
            let mut table_area = data_area;
            if state.is_drilled_down() {
                if let Some(ref key_values) = state.drilled_down_group_key {
                    let breadcrumb_layout = Layout::default()
                        .direction(Direction::Vertical)
                        .constraints([Constraint::Length(3), Constraint::Fill(1)])
                        .split(data_area);

                    let empty_vec = Vec::new();
                    let key_columns = state
                        .drilled_down_group_key_columns
                        .as_ref()
                        .unwrap_or(&empty_vec);
                    let breadcrumb_parts: Vec<String> = key_columns
                        .iter()
                        .zip(key_values.iter())
                        .map(|(col, val)| format!("{}={}", col, val))
                        .collect();
                    let breadcrumb_text = format!(
                        "← Group: {} (Press Esc to go back)",
                        breadcrumb_parts.join(" | ")
                    );

                    Block::default()
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .border_style(Style::default().fg(ctx.keybind_hints))
                        .title("Breadcrumb")
                        .render(breadcrumb_layout[0], buf);

                    let inner = Block::default().inner(breadcrumb_layout[0]);
                    Paragraph::new(breadcrumb_text)
                        .style(
                            Style::default()
                                .fg(ctx.keybind_hints)
                                .add_modifier(Modifier::BOLD),
                        )
                        .wrap(ratatui::widgets::Wrap { trim: true })
                        .render(inner, buf);

                    table_area = breadcrumb_layout[1];
                }
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
                .with_alternate_row_bg(ctx.alternate_row_color);
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
                let mut info_widget = DataTableInfo::new(
                    state,
                    info_ctx,
                    &mut app.info_modal,
                    ctx.modal_border,
                    ctx.modal_border_active,
                    ctx.text_primary,
                );
                info_widget.render(sort_area, buf);
            }
        }
        None => {
            Paragraph::new("No data loaded").render(main_area, buf);
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
        crate::render::input_strip::render(input_area, buf, app, has_error, &err_msg, ctx);
    }

    if app.sort_filter_modal.active {
        crate::render::sort_filter_sidebar::render(sort_area, buf, &mut app.sort_filter_modal, ctx);
    }

    if app.template_modal.active {
        crate::render::template_sidebar::render(sort_area, buf, app, ctx);
    }

    if app.pivot_melt_modal.active {
        pivot_melt::render_shell(
            sort_area,
            buf,
            &mut app.pivot_melt_modal,
            ctx.modal_border,
            ctx.modal_border_active,
            ctx.text_primary,
            ctx.text_inverse,
        );
    }

    if app.export_modal.active {
        let modal_width = (area.width * 3 / 4).min(80);
        let modal_height = 20;
        let modal_x = (area.width.saturating_sub(modal_width)) / 2;
        let modal_y = (area.height.saturating_sub(modal_height)) / 2;
        let modal_area = Rect {
            x: modal_x,
            y: modal_y,
            width: modal_width,
            height: modal_height,
        };
        export::render_export_modal(
            modal_area,
            buf,
            &mut app.export_modal,
            ctx.modal_border,
            ctx.modal_border_active,
            ctx.text_primary,
            ctx.text_inverse,
        );
    }
}
